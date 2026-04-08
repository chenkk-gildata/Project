# -*- mode: python ; coding: utf-8 -*-
"""
职工构成比对系统 - PyInstaller打包配置文件

使用方法:
    pyinstaller staff_composition.spec

打包后:
    - 可执行文件位于 dist/ 目录
    - 将 prompt_Staff.md 复制到可执行文件同目录
    - 运行时日志和报告将输出到可执行文件所在目录
"""

python_files = [
    'staff_composition.py',
    'config.py',
    'ai_service_enhanced.py',
    'database_manager.py',
    'logger_config.py',
]

datas = []

hiddenimports = [
    'pandas',
    'pyodbc',
    'openai',
    'tenacity',
    'tenacity.stop',
    'tenacity.wait',
    'tenacity.retry',
    'concurrent.futures',
    'threading',
    'json',
    'datetime',
    'pathlib',
    'typing',
    'dataclasses',
]

excludes = [
    'matplotlib',
    'IPython',
    'PyQt5',
    'PyQt6',
    'PySide2',
    'PySide6',
    'tkinter',
    'pytest',
    'unittest',
    'doctest',
    'pylint',
    'flake8',
    'black',
    'sphinx',
    'numpy.f2py',
    'scipy',
    'PIL',
    'cv2',
    'torch',
    'tensorflow',
    'keras',
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
    name='职工构成小程序比对 V1.1',
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
