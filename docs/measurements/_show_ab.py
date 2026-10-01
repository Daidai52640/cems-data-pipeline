import json

p = r'docs/measurements/nginx_redis_run1.json'
d = json.load(open(p, encoding='utf-8'))
rounds = d['rounds']
off, on = rounds['cache_off'], rounds['cache_on']

print('=== 1) 反代路由（经 nginx :80）===')
for k, v in d['routes'].items():
    print('  %-26s HTTP %s  bytes=%-9d %6.2f ms' % (k, v['status'], v['bytes'], v['ms']))

print()
print('=== 2) 反代层开销（同接口、同次数）===')
for k, v in d['overhead'].items():
    a, b = v['direct_5001'], v['nginx_80']
    print('  %-26s direct p50=%6.2f p95=%6.2f | nginx p50=%6.2f p95=%6.2f | d_p50=%+5.2f | bytes same=%s'
          % (k, a['ms']['p50'], a['ms']['p95'], b['ms']['p50'], b['ms']['p95'],
             v['delta_p50_ms'], a['bytes'] == b['bytes']))

print()
print('=== 3) 缓存 A/B（都走 nginx，唯一变量是 CACHE_ENABLED）===')
for label, r in (('cache_off', off), ('cache_on', on)):
    print('  [%s] enabled=%s  http=%d  TDengine_queries=%d  delta=%s'
          % (label, r['cache_enabled'], r['http_requests_total'],
             r['tdengine_queries_during_round'], json.dumps(r['counters_delta'])))

print()
print('%-32s %5s %-9s | %-17s | %-17s | %s' % ('workload', 'n', 'cacheable', 'cache_off p50/p95', 'cache_on p50/p95', 'delta_p50'))
for k in off['results']:
    a = off['results'][k]['ms']
    b = on['results'][k]['ms']
    print('%-32s %5d %-9s | %7.2f /%7.2f | %7.2f /%7.2f | %+7.2f'
          % (k, off['results'][k]['n'], off['results'][k]['cacheable'],
             a['p50'], a['p95'], b['p50'], b['p95'], b['p50'] - a['p50']))

print()
print('=== 4) 每个负载的缓存增量（cache_on 轮）===')
for k, v in on['results'].items():
    print('  %-32s %s' % (k, json.dumps(v['counters_delta'])))

print()
print('=== 5) 命中率口径 ===')
cd = on['counters_delta']
h, m, s, bf = cd['hit'], cd['miss'], cd['stale'], cd['bypass_future']
lookups = h + m + s
print('  hit=%d miss=%d stale=%d | 可缓存查询=%d | hit_rate=%.4f' % (h, m, s, lookups, (h / lookups) if lookups else 0))
cacheable_requests = sum(v['n'] for v in on['results'].values() if v['cacheable'])
print('  可缓存负载请求数=%d | 实际查库次数(增量)=%d | 下降=%.1f%%'
      % (cacheable_requests, cd['query'], 100.0 * (1 - cd['query'] / cacheable_requests)))
print('  bypass_future=%d（窗口未闭合，按设计直连）| watermarks_after=%s'
      % (bf, json.dumps(on['watermarks_after'], ensure_ascii=False)))
print('  events=%s' % json.dumps(on.get('events', {}), ensure_ascii=False))
