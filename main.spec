# -*- mode: python ; coding: utf-8 -*-
import secrets

block_cipher = None

# 生成加密密钥
ENCRYPT_KEY = secrets.token_bytes(16)

# Paramiko 及其依赖的隐藏导入（简化版）
paramiko_hiddenimports = [
    'paramiko',
    'paramiko.transport',
    'paramiko.client',
    'paramiko.ssh_exception',
    'paramiko.auth_handler',
    'paramiko.channel',
    'paramiko.message',
    'paramiko.pkey',
    'paramiko.rsakey',
    'paramiko.dsskey',
    'paramiko.ecdsakey',
    'paramiko.ed25519key',
    'paramiko.sftp',
    'paramiko.sftp_client',
    'paramiko.agent',
    # cryptography 相关依赖
    '_cffi_backend',
    'cryptography',
    'cryptography.hazmat.backends.openssl',
    'cryptography.hazmat.primitives.ciphers',
    'cryptography.hazmat.primitives.asymmetric',
    'cryptography.hazmat.primitives.serialization',
    'cryptography.hazmat.primitives.hashes',
    'cryptography.hazmat.bindings.openssl',
    # bcrypt 和 pynacl
    'bcrypt',
    'nacl',
]

a = Analysis(['main.py'],
             pathex=['.'],
             binaries=[],
             datas=[],
             hiddenimports=paramiko_hiddenimports,
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data,
             cipher=None,
             key=ENCRYPT_KEY)

exe = EXE(pyz,
          a.scripts,
          a.binaries,  # 添加二进制文件
          a.zipfiles,  # 添加zip文件
          a.datas,     # 添加数据文件
          name='MGR.BIN',  # 输出文件名
          debug=False,
          strip=False,
          upx=True,    # 使用UPX压缩，文件更小
          console=True,  # 控制台程序
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None,
          # onefile 模式配置
          onefile=True)  # 关键：设置为True生成单个文件
