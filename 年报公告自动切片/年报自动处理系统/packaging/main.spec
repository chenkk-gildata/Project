# -*- mode: python ; coding: utf-8 -*-
"""
年报公告自动处理系统 - PyInstaller打包配置
主程序打包配置文件
"""

import os

spec_dir = os.path.dirname(os.path.abspath(SPEC))
project_root = os.path.dirname(spec_dir)

python_files = [
    os.path.join(project_root, 'main.py'),
    os.path.join(project_root, 'config.py'),
    os.path.join(project_root, 'database.py'),
    os.path.join(project_root, 'downloader.py'),
    os.path.join(project_root, 'logger.py'),
    os.path.join(project_root, 'models.py'),
    os.path.join(project_root, 'monitor.py'),
    os.path.join(project_root, 'queues.py'),
    os.path.join(project_root, 'task_dispatcher.py'),
    os.path.join(project_root, 'processors', '__init__.py'),
    os.path.join(project_root, 'processors', 'base_processor.py'),
    os.path.join(project_root, 'processors', 'zyzb_processor.py'),
    os.path.join(project_root, 'processors', 'ldrjs_processor.py'),
    os.path.join(project_root, 'processors', 'yftr_processor.py'),
    os.path.join(project_root, 'processors', 'zggc_processor.py'),
    os.path.join(project_root, 'processors', 'ldrcg_processor.py'),
    os.path.join(project_root, 'utils', '__init__.py'),
    os.path.join(project_root, 'utils', 'pdf_utils.py'),
]

datas = [
    (os.path.join(project_root, 'query.sql'), '.'),
]

hiddenimports = [
    'pyodbc',
    'requests',
    'fitz',
    'PyPDF2',
    'sqlite3',
    'queue',
    'threading',
    'concurrent.futures',
    'datetime',
    'dataclasses',
    'enum',
    'contextlib',
    'signal',
    'atexit',
    'ctypes',
    're',
    'json',
    'logging',
    'logging.handlers',
    'config',
    'database',
    'logger',
    'models',
    'monitor',
    'downloader',
    'queues',
    'task_dispatcher',
    'processors',
    'processors.base_processor',
    'processors.zyzb_processor',
    'processors.ldrjs_processor',
    'processors.yftr_processor',
    'processors.zggc_processor',
    'processors.ldrcg_processor',
    'utils',
    'utils.pdf_utils',
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
    name='AnnualReportProcessor',
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
