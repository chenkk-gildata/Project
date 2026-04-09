# -*- mode: python ; coding: utf-8 -*-
"""
主要指标一季报AI比对系统 - PyInstaller打包配置文件

使用方法:
    pyinstaller main_indicator.spec

打包后的目录结构:
    dist/
    └── main_indicator.exe
    
运行时需要的文件:
    - 主要指标季度报告.md (提示词文件，放在exe同目录)
    
运行时生成的目录:
    - files/  (下载的公告)
    - logs/   (日志文件)
    - reports/(比对报告)
"""

import sys
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

block_cipher = None

hidden_imports = [
    'pandas',
    'openpyxl',
    'openpyxl.styles',
    'openpyxl.cell._writer',
    'requests',
    'urllib3',
    'charset_normalizer',
    'charset_normalizer.md',
    'idna',
    'idna.idnadata',
    'pyodbc',
    'concurrent.futures',
    'concurrent.futures.thread',
    'json',
    'logging',
    'logging.handlers',
    'threading',
    'queue',
]

datas = []

a = Analysis(
    ['main_indicator.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy.f2py',
        'IPython',
        'jupyter',
        'notebook',
        'PIL',
        'PyQt5',
        'PyQt6',
        'PySide2',
        'PySide6',
        'scipy',
        'pytest',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='主要指标一季报AI比对系统',
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
    icon=None,
)
