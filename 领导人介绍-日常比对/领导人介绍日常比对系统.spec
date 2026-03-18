# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[('LDRJS_mappings.py', '.')],
    hiddenimports=['pandas', 'pyodbc', 'requests', 'openai', 'concurrent.futures', 'queue', 'tkinter', 'tkinter.filedialog'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'IPython', 'jedi', 'parso', 'PyQt5', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 
               'nbformat', 'jsonschema', 'lark', 'wcwidth', 'pygments', 'zmq', 'pycparser',
               'setuptools', 'pkg_resources', 'wheel', 'importlib_metadata', 'more_itertools',
               'jaraco.text', 'jaraco.functools', 'jaraco.context', 'backports.tarfile'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='领导人介绍日常比对系统 V2.2',
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
)
