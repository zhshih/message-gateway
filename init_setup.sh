#!/bin/bash

set -e

# ===== CONFIGURATION =====
BASE_DIR="certs"
DAYS_VALID=365
RSA_KEY_SIZE=2048

COMPONENTS=("mqtt" "redis")
CERT_TARGETS=("config/mosquitto/certs" "config/redis/certs")
CLIENT_TARGET="config/message-gateway/certs"

declare -A CERT_UIDS
CERT_UIDS=( ["mqtt"]=1883 ["redis"]=999 )

#######################################
# Create necessary directories
#######################################
prepare_directories() {
  for component in "${COMPONENTS[@]}"; do
    mkdir -p "$BASE_DIR/$component"
  done
}

#######################################
# Clean all certs and config outputs
#######################################
clean_certs() {
  echo "Cleaning up certificate directories..."

  sudo rm -rf "$BASE_DIR"

  for target in "${CERT_TARGETS[@]}" "$CLIENT_TARGET"; do
    [ -d "$target" ] && sudo rm -rf "$target"
  done

  echo "Certificate cleanup complete."
}

#######################################
# Generate Root CA for a given component
# Arguments:
#   $1 - Component name (e.g., mqtt or redis)
#######################################
generate_root_ca() {
  local component=$1
  local ca_dir="$BASE_DIR/$component/ca"
  local cn="RootCA-${component}"

  mkdir -p "$ca_dir"

  openssl genrsa -out "$ca_dir/ca.key" $RSA_KEY_SIZE
  openssl req -x509 -new -nodes -key "$ca_dir/ca.key" -sha256 -days 3650 \
    -out "$ca_dir/ca.crt" -subj "/C=US/ST=State/L=City/O=MyOrg/OU=IT/CN=${cn}"
}

#######################################
# Generate certs for server/client
# Arguments:
#   $1 - Component name
#   $2 - Role: server or client
#   $3 - CN
#   $4 - Usage (serverAuth or clientAuth)
#   $5 - Optional DNS SAN
#######################################
generate_certificate() {
  local component=$1
  local role=$2
  local cn=$3
  local usage=$4
  local dns_san=${5:-}

  local cert_dir="$BASE_DIR/$component"
  local ca_dir="$cert_dir/ca"
  local config_file="$cert_dir/openssl_${role}_ext.cnf"
  local key_file="$cert_dir/${role}.key"
  local csr_file="$cert_dir/${role}.csr"
  local crt_file="$cert_dir/${role}.crt"

  cat > "$config_file" <<EOF
[ req ]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[ req_distinguished_name ]
C = US
ST = State
L = City
O = MyOrg
OU = IT
CN = $cn

[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = $usage
EOF

  if [ -n "$dns_san" ]; then
    cat >> "$config_file" <<EOF

subjectAltName = @alt_names

[ alt_names ]
DNS.1 = $dns_san
EOF
  fi

  openssl genrsa -out "$key_file" $RSA_KEY_SIZE
  openssl req -new -key "$key_file" -out "$csr_file" -config "$config_file"
  openssl x509 -req -in "$csr_file" -CA "$ca_dir/ca.crt" -CAkey "$ca_dir/ca.key" \
    -CAcreateserial -out "$crt_file" -days $DAYS_VALID -sha256 \
    -extfile "$config_file" -extensions v3_req
}

#######################################
# Verifies that a cert and key match
# Arguments:
#   $1 - Component name
#   $2 - Role: server or client
#######################################
verify_cert_key_pair() {
  local component=$1
  local role=$2

  local cert_file="$BASE_DIR/$component/${role}.crt"
  local key_file="$BASE_DIR/$component/${role}.key"

  local cert_mod
  local key_mod

  cert_mod=$(openssl x509 -noout -modulus -in "$cert_file" | openssl md5)
  key_mod=$(openssl rsa -noout -modulus -in "$key_file" | openssl md5)

  if [ "$cert_mod" != "$key_mod" ]; then
    echo "ERROR: Certificate and key do not match for $component ($role)"
    exit 1
  else
    echo "SUCCESS: Certificate and key match for $component ($role)"
  fi
}

#######################################
# Deploy server certs to service config
# Arguments:
#   $1 - Component name
#   $2 - Target path
#   $3 - UID
#######################################
deploy_server_certificates() {
  local component=$1
  local target_dir=$2
  local uid=$3

  local cert_dir="$BASE_DIR/$component"

  sudo mkdir -p "$target_dir"
  sudo cp "$cert_dir/ca/ca.crt" "$cert_dir/server.crt" "$cert_dir/server.key" "$target_dir/"
  sudo chown "$uid:$uid" "$target_dir/server.key"
  sudo chmod 600 "$target_dir/server.key"
}

#######################################
# Deploy client certs
# Arguments:
#   $1 - Component name
#   $2 - Target base directory
#   $3 - Optional UID
#######################################
deploy_client_certificates() {
  local component=$1
  local target_base=$2
  local uid=${3:-}
  local target_dir="$target_base/$component"
  local cert_dir="$BASE_DIR/$component"

  sudo mkdir -p "$target_dir"
  sudo cp "$cert_dir/ca/ca.crt" "$cert_dir/client.crt" "$cert_dir/client.key" "$target_dir/"
  sudo chmod 644 "$target_dir/client.key"

  if [ -n "$uid" ]; then
    sudo chown "$uid:$uid" "$target_dir/client.crt" "$target_dir/client.key"
  fi
}

#######################################
# Main Execution
#######################################
main() {
  clean_certs
  prepare_directories

  for component in "${COMPONENTS[@]}"; do
    echo "Generating CA for $component..."
    generate_root_ca "$component"

    echo "Generating server certificate for $component..."
    generate_certificate "$component" "server" "$component" "serverAuth" "$component"

    echo "Generating client certificate for $component..."
    generate_certificate "$component" "client" "${component}-client" "clientAuth" "${component}-client"

    echo "Verifying certificate/key pair for $component..."
    verify_cert_key_pair "$component" "server"
  done

  echo "Deploying certificates..."

  deploy_server_certificates "mqtt" "${CERT_TARGETS[0]}" "${CERT_UIDS[mqtt]}"
  deploy_server_certificates "redis" "${CERT_TARGETS[1]}" "${CERT_UIDS[redis]}"

  deploy_client_certificates "mqtt" "$CLIENT_TARGET"
  deploy_client_certificates "redis" "$CLIENT_TARGET"

  cp config.toml "config/message-gateway/."
}

main
