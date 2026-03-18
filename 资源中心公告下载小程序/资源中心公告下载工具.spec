# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['资源中心公告下载.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('E:/Project/venv314/Lib/site-packages/PyQt5/Qt5/plugins', 'PyQt5/Qt5/plugins'),
        ('云下载.ico', '.')
    ],
    hiddenimports=['PyQt5.QtWidgets', 'PyQt5.QtCore', 'PyQt5.QtGui', 'pandas', 'pyodbc', 'requests', 'numpy','concurrent.futures'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    a.binaries,
    a.datas,
    name='资源中心公告下载工具',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='云下载.ico'
)