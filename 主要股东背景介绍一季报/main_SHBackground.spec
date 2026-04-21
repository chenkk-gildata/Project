# -*- mode: python ; coding: utf-8 -*-
"""
主要股东背景介绍一季报 AI 比对系统 - PyInstaller 打包配置文件

使用方法:
    pyinstaller main_SHBackground.spec
"""

block_cipher = None

hidden_imports = [
    "pandas",
    "openpyxl",
    "openpyxl.styles",
    "openpyxl.cell._writer",
    "requests",
    "urllib3",
    "charset_normalizer",
    "charset_normalizer.md",
    "idna",
    "idna.idnadata",
    "pyodbc",
    "concurrent.futures",
    "concurrent.futures.thread",
    "json",
    "logging",
    "logging.handlers",
    "threading",
    "queue",
    "openai",
    "tenacity",
    "fitz",
    "PyPDF2",
    "zhconv",
]

datas = [
    ("主要股东背景介绍一季报.md", "."),
]

a = Analysis(
    ["main_SHBackground.py"],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        "tkinter",
        "matplotlib",
        "numpy.f2py",
        "IPython",
        "jupyter",
        "notebook",
        "PIL",
        "PyQt5",
        "PyQt6",
        "PySide2",
        "PySide6",
        "scipy",
        "pytest",
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
    name="主要股东背景介绍一季报AI比对系统",
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
