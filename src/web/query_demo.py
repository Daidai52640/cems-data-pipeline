# -*- coding: utf-8 -*-
"""
TDengine 查询演示（Day 14 收尾）
功能：查入库数据 + INTERVAL 时间聚合（分钟/小时均值）
运行：python query_demo.py
      （需要 subscriber_to_td.py 在跑，库里才有数据）
"""

import taosrest

TD_URL = "http://localhost:6041"
TD_USER = "root"
TD_PASS = "taosdata"


def main():
    conn = taosrest.connect(url=TD_URL, user=TD_USER, password=TD_PASS)
    cur = conn.cursor()

    # 1. 入库总条数
    cur.execute("SELECT COUNT(*) FROM cems.cems_data")
    print(f"入库总条数: {cur.fetchone()[0]}")

    # 2. 最新5条原始数据
    cur.execute("SELECT * FROM cems.cems_data ORDER BY ts DESC LIMIT 5")
    print("\n最新5条原始数据:")
    for r in cur.fetchall():
        print(f"  {r[0]} | SO2={r[1]} NOx={r[2]} Flow={r[3]} | {r[4]}/{r[5]}")

    # 3. ★ INTERVAL 时间聚合：按1分钟窗口算 SO2 均值（环保平台"分钟均值"曲线）
    cur.execute(
        "SELECT _wstart, AVG(so2) AS avg_so2, COUNT(*) AS n "
        "FROM cems.cems_data WHERE ts >= now - 30m INTERVAL(1m)"
    )
    print("\n★ INTERVAL(1m) 分钟均值（最近30分钟，每1分钟一个窗口）:")
    for r in cur.fetchall():
        print(f"  窗口起点 {r[0]} | SO2均值 {round(r[1], 2)} | 原始条数 {r[2]}")

    # 4. 想玩：改成 INTERVAL(1h) 就是小时均值报表
    # cur.execute("SELECT _wstart, AVG(so2) FROM cems.cems_data WHERE ts >= now - 7d INTERVAL(1h)")

    conn.close()


if __name__ == "__main__":
    main()
