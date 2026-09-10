# -*- coding: utf-8 -*-
"""
Modbus 仿真设备（Server）
功能：起一个 Modbus TCP Server，寄存器里存仿真CEMS数据（SO2/NOx/Flow），随机波动
     让网关（client）来读，模拟真实设备
运行：python modbus_server.py
"""

import random
import threading
import time

from pymodbus.server import StartTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext

# ========== 寄存器地址定义（和网关约好的"内存地图"）==========
REG_SO2 = 0      # 地址0：SO2 实测值（放大10倍存：35.2 → 352）
REG_NOX = 1      # 地址1：NOx 实测值（放大10倍）
REG_FLOW = 2     # 地址2：流速（放大10倍）
REG_COUNT = 10   # 寄存器数量（多留几个备用）

# ========== 数据更新线程：每2秒让数据随机波动 ==========
def update_data(context):
    """循环更新寄存器里的仿真数据，模拟真实设备读数变化"""
    while True:
        # 生成仿真数据（真实设备读数会波动）
        so2 = round(random.uniform(20, 50), 1)    # SO2 20~50 mg/m3
        nox = round(random.uniform(10, 30), 1)    # NOx 10~30 mg/m3
        flow = round(random.uniform(80, 120), 1)  # 流速 80~120 m3/s

        # 写入寄存器（放大10倍存整数，Modbus寄存器只存整数）
        values = [
            int(so2 * 10),   # 地址0：SO2
            int(nox * 10),   # 地址1：NOx
            int(flow * 10),  # 地址2：Flow
            0, 0, 0, 0, 0, 0, 0,  # 备用寄存器
        ]

        # 写入从站1的保持寄存器（从地址0开始）
        context[1].setValues(3, REG_SO2, values)  # 3=保持寄存器
        print(f"[仿真设备] SO2={so2}mg/m3 NOx={nox}mg/m3 Flow={flow}m3/s")
        time.sleep(2)   # 每2秒更新一次


def main():
    # 初始化寄存器数据块（地址0~9，初始全0）
    block = ModbusSequentialDataBlock(0, [0] * REG_COUNT)
    # 从站1（unit_id=1）
    slave = ModbusSlaveContext(di=block, co=block, hr=block, ir=block)
    context = ModbusServerContext(slaves=slave, single=True)

    # 后台线程：持续更新仿真数据
    t = threading.Thread(target=update_data, args=(context,), daemon=True)
    t.start()

    # 启动 Modbus TCP Server（端口5020，避开系统502可能被占用）
    print("Modbus 仿真设备启动: localhost:5020, 从站地址=1")
    print("寄存器地图: 地址0=SO2(×10), 地址1=NOx(×10), 地址2=Flow(×10)")
    StartTcpServer(context=context, address=("0.0.0.0", 5020))


if __name__ == "__main__":
    main()
