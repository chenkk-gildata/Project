# -*- mode: python ; coding: utf-8 -*-
"""
申报稿领导人AI处理系统 - PyInstaller打包配置
使用方法: pyinstaller prospectus_leader.spec
"""

block_cipher = None

a = Analysis(
    ['prospectus_leader.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('daily.sql', '.'),
        ('compare.sql', '.'),
    ],
    hiddenimports=[
        'openpyxl',
        'openpyxl.cell.text',
        'openpyxl.cell.rich_text',
        'openpyxl.styles',
        'pandas',
        'requests',
        'tenacity',
        'difflib',
        'concurrent.futures',
    ],
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
    name='申报稿领导人AI处理系统 V1.0',
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
