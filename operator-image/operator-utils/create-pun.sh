#!/usr/bin/bash
shift
while [ $# -gt 0 ]; do
  case "$1" in
    --user*|-u*)
      if [[ "$1" != *=* ]]; then shift; fi # Value is next arg if no `=`
      OOD_USER="${1#*=}"
      ;;
    --app-init-url*|-a*)
      if [[ "$1" != *=* ]]; then shift; fi
      app_init_url="${1#*=}"
      ;;
    --help|-h)
      printf "Meaningful help message" # Flag argument
      exit 0
      ;;
    *)
      >&2 printf "Error: Invalid argument\n"
      exit 1
      ;;
  esac
  shift
done

export DNS_OOD_USER="${OOD_USER//\./"-"}"
export OOD_USER
export NAMESPACE=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace)
export OOD_INSTANCE=$(cat /opt/krood/labels/ood-cluster)
curl -X POST  -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" -H "Content-Type: application/yaml" --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt --data-binary @<(envsubst < /opt/krood/utils/templates/pun.yaml ) https://kubernetes.default.svc/apis/ondemand.krood.dev/v1alpha1/namespaces/$NAMESPACE/puns
