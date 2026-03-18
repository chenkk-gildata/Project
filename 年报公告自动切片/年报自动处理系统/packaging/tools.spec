# -*- mode: python ; coding: utf-8 -*-
"""
年报公告自动处理系统 - PyInstaller打包配置
工具程序打包配置文件
"""

import os

spec_dir = os.path.dirname(os.path.abspath(SPEC))
project_root = os.path.dirname(spec_dir)

python_files = [
    os.path.join(project_root, 'tools', 'tools_menu.py'),
    os.path.join(project_root, 'tools', '__init__.py'),
    os.path.join(project_root, 'tools', 'announcement_viewer.py'),
    os.path.join(project_root, 'tools', 'status_viewer.py'),
    os.path.join(project_root, 'config.py'),
    os.path.join(project_root, 'database.py'),
    os.path.join(project_root, 'logger.py'),
    os.path.join(project_root, 'models.py'),
]

datas = []

hiddenimports = [
    'sqlite3',
    'datetime',
    'dataclasses',
    'enum',
    'contextlib',
    're',
    'json',
    'logging',
    'logging.handlers',
    'prettytable',
    'readchar',
    'config',
    'database',
    'logger',
    'models',
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
    'numpy',
    'pandas',
    'scipy',
    'PIL',
    'cv2',
    'pyodbc',
    'requests',
    'fitz',
    'PyPDF2',
]

a = Analysis(
    python_files,
    pathex=[project_root],
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
    name='StatusViewer',
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
