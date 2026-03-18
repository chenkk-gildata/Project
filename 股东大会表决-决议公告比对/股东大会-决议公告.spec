# -*- mode: python ; coding: utf-8 -*-
import sys
import os

block_cipher = None

a = Analysis(
    ['main_voting.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config.py', '.'),
        ('logger_config.py', '.'),
        ('cninfo_Crawling_usrGDDHBJ.py', '.'),
        ('ai_service_enhanced.py', '.'),
        ('database_manager.py', '.'),
        ('mappings_voting.py', '.'),
        ('prompt_GDDHBJ.md', '.'),
        ('prompt_GDDHCX_HBBJ.md', '.')
    ],
    hiddenimports=[
        # 核心模块
        'config',
        'logger_config', 
        'database_manager',
        'ai_service_enhanced',
        'mappings_voting',
        'cninfo_Crawling_usrGDDHBJ',
        
        # Python 3.14 兼容性修复
        'collections.abc',
        'typing',
        'tracemalloc',
        'traceback',
        '_colorize',
        'logging',
        'selectors',
        'http.client',
        'importlib.resources.readers',
        'inspect',
        'multiprocessing.managers',
        
        # AI和HTTP相关（精简版）
        'openai',
        'httpx',
        'requests',
        'urllib3',
        'charset_normalizer',
        
        # 数据处理相关（精简版）
        'pandas',
        'pandas.io.formats',
        'pandas.io.formats.style',
        'pandas.io.clipboard',
        'openpyxl',
        'xlrd',
        'xlsxwriter',
        
        # 数据库相关
        'pyodbc',
        'sqlite3',
        
        # XML和HTML解析（精简版）
        'xml.etree',
        'xml.etree.cElementTree',
        'lxml',
        'lxml.etree',
        'lxml.html',
        
        # 图像处理（精简版）
        'PIL',
        'PIL.Image',
        'PIL.ImageFilter',
        'PIL.ImageMode',
        'PIL.PngImagePlugin',
        
        # 日期时间处理
        'dateutil',
        'dateutil.parser',
        'pytz',
        
        # 重试和异步（精简版）
        'tenacity',
        'concurrent.futures',
        'threading',
        
        # 其他必要模块
        'json',
        're',
        'os',
        'sys',
        'pathlib',
        'datetime',
        'time',
        'random',
        'logging',
        'configparser',
        
        # Pydantic相关
        'pydantic_core',
        'pydantic',
        'pydantic_core._pydantic_core',
        'pydantic_core.core_schema',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    name='股东大会-决议公告比对V2.1',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    upx_exclude=[],
    runtime_tmpdir=None,
    # 调试期间设为True，可以看到错误信息
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None
)