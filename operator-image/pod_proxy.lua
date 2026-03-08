local user_map    = require 'ood.user_map'
local proxy       = require 'ood.proxy'
local http        = require 'ood.http'
local nginx_stage = require 'ood.nginx_stage'
local socket = require 'socket'

function is_pod_listening(host, port)
    local tcp = assert(socket.tcp())
    tcp:settimeout(1)  -- short timeout for fast failure
    local success, err = tcp:connect(host, port)
    if success then
        tcp:close()
        return true
    else
        return false
    end
end

--[[
  pun_proxy_handler

  Maps an authenticated user to a system user. Then proxies user's traffic to
  user's backend PUN through a Unix domain socket. If the backend PUN is down,
  then launch the user's PUN through a redirect.
--]]
function pun_proxy_handler(r)
  -- read in OOD specific settings defined in Apache config
  local user_map_match        = r.subprocess_env['OOD_USER_MAP_MATCH']
  local user_map_cmd          = r.subprocess_env['OOD_USER_MAP_CMD']
  local user_env              = r.subprocess_env['OOD_USER_ENV']
  local pun_socket_root       = r.subprocess_env['OOD_PUN_SOCKET_ROOT']
  local nginx_uri             = r.subprocess_env['OOD_NGINX_URI']
  local map_fail_uri          = r.subprocess_env['OOD_MAP_FAIL_URI']
  local pun_stage_cmd         = r.subprocess_env['OOD_PUN_STAGE_CMD']
  local pun_pre_hook_exports  = r.subprocess_env['OOD_PUN_PRE_HOOK_EXPORTS']
  local pun_pre_hook_root_cmd = r.subprocess_env['OOD_PUN_PRE_HOOK_ROOT_CMD']
  local allowed_hosts         = r.subprocess_env['OOD_ALLOWED_HOSTS']
  local pun_max_retries       = tonumber(r.subprocess_env['OOD_PUN_MAX_RETRIES'])
  local pun_port              = 80

  -- get the system-level user name
  local user = user_map.map(r, user_map_match, user_map_cmd, user_env and r.subprocess_env[user_env] or r.user)
  if not user then
    if map_fail_uri then
      return http.http302(r, map_fail_uri .. "?redir=" .. r:escape(r.unparsed_uri))
    else
      return http.http404(r, "failed to map user (" .. r.user .. ")")
    end
  end

  -- generate connection object used in setting the reverse proxy
  local conn = {}
  conn.user = user
  local dns_user = user:gsub("%.","-")
  local ood_name = os.getenv("KROOD_OOD_NAME")
  local pun_dns_name = "nginx-" .. ood_name .. "-" .. dns_user
  conn.server = pun_dns_name .. ":" .. tostring(pun_port)
  conn.uri = r.uri

  -- start up PUN if socket doesn't exist
  local count = 0
  local app_init_url = r.is_https and "https://" or "http://"
  app_init_url = app_init_url .. r.hostname .. ":" .. r.port .. nginx_uri .. "/init?redir=$http_x_forwarded_escaped_uri"
  local ok = is_pod_listening(pun_dns_name,pun_port)
  if not ok then
        nginx_stage.pun(r, pun_stage_cmd, user, app_init_url, pun_pre_hook_exports, pun_pre_hook_root_cmd, allowed_hosts)
        while not is_pod_listening(pun_dns_name,pun_port) and count < pun_max_retries do
            r.usleep(150000)
            count = count + 1
        end
  end
  if count == pun_max_retries then
      return http404(r, "Too long waiting for pun to start")
  end
  -- setup request for reverse proxy
  proxy.set_reverse_proxy(r, conn)

  -- let the proxy handler do this instead
  return apache2.DECLINED
end
