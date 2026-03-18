# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller打包配置文件
业绩预告AI比对系统

使用方法:
    pyinstaller PerformFore.spec

生成的可执行文件将包含所有依赖和资源文件
"""

block_cipher = None

# 获取当前目录
import os
current_dir = os.path.dirname(os.path.abspath(SPEC))

a = Analysis(
    ['main_PerformFore.py'],
    pathex=[current_dir],
    binaries=[],
    datas=[
        # 包含提示词文件
        ('业绩预告优化.md', '.'),
        # 包含配置文件
        ('config_PerformFore.py', '.'),
        # 包含路径工具模块
        ('path_utils.py', '.'),
        # 包含数据库管理模块
        ('database_manager.py', '.'),
        # 包含AI服务模块
        ('ai_service_enhanced.py', '.'),
        # 包含日志配置模块
        ('logger_config.py', '.'),
    ],
    hiddenimports=[
        # 核心依赖
        'pyodbc',
        'openai',
        'requests',
        'pandas',
        'openpyxl',
        'tenacity',
        'concurrent.futures',
        'concurrent.futures.thread',
        'concurrent.futures.process',
        # tkinter相关
        'tkinter',
        # JSON处理
        'json',
        # 日期时间
        'datetime',
        # 正则表达式
        're',
        # 操作系统相关
        'os',
        'sys',
        'threading',
        'logging',
        'logging.handlers',
        # 路径处理
        'pathlib',
        # 异常处理
        'traceback',
        'contextlib',
        'typing',
    ],
    hookspath=[],
    hooksconfig=[],
    runtime_hooks=[],
    excludes=[
        # 排除测试相关模块
        'test',
        'tests',
        'unittest',
        'pytest',
        'doctest',
        # 排除开发工具
        'pdb',
        'ipdb',
        'pydevd',
        # 排除不常用的GUI工具
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'wx',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# 收集所有文件
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='业绩预告AI比对系统 V1.7',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # 保持控制台窗口，便于查看日志
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # 如需图标，设置为图标文件路径，如 'icon.ico'
)

# 如果需要生成无控制台的Windows GUI程序，注释上面的exe，启用下面的块：

# exe = EXE(
#     pyz,
#     a.scripts,
#     [],
#     exclude_binaries=True,
#     name='业绩预告AI比对系统',
#     debug=False,
#     bootloader_ignore_signals=False,
#     strip=False,
#     upx=True,
#     console=False,  # 无控制台窗口
#     disable_windowed_traceback=False,
#     argv_emulation=False,
#     target_arch=None,
#     codesign_identity=None,
#     entitlements_file=None,
#     icon=None,
# )
#
# coll = COLLECT(
#     exe,
#     a.binaries,
#     a.zipfiles,
#     a.datas,
#     strip=False,
#     upx=True,
#     upx_exclude=[],
#     name='业绩预告AI比对系统',
# )
