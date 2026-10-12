"""北向协议实现（HJ 212 及后续协议）。

hj212/ 是 HJ 212-2025 的编解码实现（报文 + CRC16 + SM4 + 分帧）。
出口适配接口 ProtocolAdapter 及三个实现（HJ212 / HTTP-JSON / Modbus-TCP）
在同包的 adapter.py 里，ADR-0006 §6"先有实现再抽接口"已满足。
"""
