#!/bin/sh
set -eu

# LDAP auth is optional. If LDAP_SERVER is blank, strip the LDAP
# authenticator variables so EMQX starts with built-in-database auth only.
if [ -z "${LDAP_SERVER:-}" ]; then
  unset EMQX_AUTHENTICATION__2__MECHANISM
  unset EMQX_AUTHENTICATION__2__BACKEND
  unset EMQX_AUTHENTICATION__2__METHOD__TYPE
  unset EMQX_AUTHENTICATION__2__METHOD__BIND_PASSWORD
  unset EMQX_AUTHENTICATION__2__SERVER
  unset EMQX_AUTHENTICATION__2__USERNAME
  unset EMQX_AUTHENTICATION__2__PASSWORD
  unset EMQX_AUTHENTICATION__2__BASE_DN
  unset EMQX_AUTHENTICATION__2__FILTER
  unset EMQX_AUTHENTICATION__2__QUERY_TIMEOUT
fi

# Preserve the broker startup command even if the wrapped image CMD is not
# carried through by the build/runtime path.
if [ "$#" -eq 0 ]; then
  set -- /opt/emqx/bin/emqx foreground
fi

exec /usr/bin/docker-entrypoint.sh "$@"
