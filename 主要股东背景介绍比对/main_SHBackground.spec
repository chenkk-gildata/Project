# -*- mode: python ; coding: utf-8 -*-

import os
import sys
from pathlib import Path

# 获取当前目录
current_dir = os.path.dirname(os.path.abspath(SPEC))

# 主要入口文件
main_script = os.path.join(current_dir, 'main_SHBackground.py')

# 收集所有需要的数据文件
added_files = [
    # 配置文件
    ('config.py', '.'),
    
    # 其他Python模块
    ('database_manager.py', '.'),
    ('ai_service_enhanced.py', '.'),
    ('logger_config.py', '.'),
    
    # 提示词文件
    ('主要股东背景介绍.md', '.'),
    
    # 数据文件目录
    ('主要股东背景介绍小程序比对', '主要股东背景介绍小程序比对'),
    
    # zhconv模块的字典文件
    ('D:\\Pyhon\\python-3.12.0\\Lib\\site-packages\\zhconv\\zhcdict.json', 'zhconv'),
]

# 隐藏导入（可能不会被自动检测的模块）
hiddenimports = [
    'pandas',
    'numpy',
    'openai',
    'pyodbc',
    'zhconv',
    'tenacity',
    'concurrent.futures',
    'threading',
    'json',
    'pathlib',
    'typing',
    'datetime',
    're',
    'dataclasses',
    'contextlib',
    'logging',
    'time',
    'random',
    'os',
    'sys',
]

# 排除不需要的模块
excludes = [
    'matplotlib',
    'scipy',
    'IPython',
    'jupyter',
    'notebook',
    'tkinter',
    'PyQt5',
    'PyQt6',
    'PySide2',
    'PySide6',
]

# 分析配置
a = Analysis(
    [main_script],
    pathex=[current_dir],
    binaries=[],
    datas=added_files,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
)

# 去除不必要的二进制文件，减小体积
a.binaries = [x for x in a.binaries if not x[0].startswith('tcl')]
a.binaries = [x for x in a.binaries if not x[0].startswith('tk')]

# PYZ配置
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# 可执行文件配置
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='主要股东背景介绍比对系统',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # 如果有图标文件，可以在这里指定路径
)