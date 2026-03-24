# -*- mode: python ; coding: utf-8 -*-
import os
import sys

block_cipher = None

python_files = [
    'main_indicator.py',
    'config.py',
    'database_manager.py',
    'ai_service_enhanced.py',
    'logger_config.py',
]

datas = []

hiddenimports = [
    'pandas',
    'pyodbc',
    'openai',
    'tenacity',
    'tkinter',
    'tkinter.filedialog',
    'tkinter.messagebox',
    'openpyxl',
    'xlrd',
    'xlwt',
    'urllib3',
    'requests',
]

excludes = [
    'matplotlib',
    'IPython',
    'PyQt5',
    'pytest',
    'unittest',
    'doctest',
    'pylint',
    'flake8',
    'black',
]

a = Analysis(
    python_files,
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=excludes,
    noarchive=False,
    optimize=2,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='主要指标年报AI比对系统 V1.1',
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
