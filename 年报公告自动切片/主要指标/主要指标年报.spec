# -*- mode: python ; coding: utf-8 -*-

python_files = [
    '主要指标年报.py',
]

datas = []

hiddenimports = [
    'fitz',
    'PyPDF2',
    'PyPDF2.pdf',
    'PyPDF2.reader',
    'PyPDF2.writer',
    'PyPDF2.generic',
    'PyPDF2.utils',
    're',
    'os',
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
    'numpy.f2py',
    'numpy.distutils',
    'scipy',
    'pandas',
    'PIL',
    'cv2',
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
    name='主要指标年报切片小程序',
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
