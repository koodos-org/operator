set -e
APISERVER="https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT}"
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
CA_CERT_PATH="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
KCFG_PATH="/etc/kubeconfig"


cat > "$KCFG_PATH" <<EOF
apiVersion: v1
kind: Config
clusters:
- name: in-cluster
  cluster:
    server: ${APISERVER}
    certificate-authority: ${CA_CERT_PATH}
users:
- name: sa-user
  user:
    token: ${TOKEN}
contexts:
- name: in-cluster
  context:
    cluster: in-cluster
    user: sa-user
current-context: in-cluster
EOF
