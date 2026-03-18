# -*- mode: python ; coding: utf-8 -*-
"""
股东股权变动AI比对系统 - PyInstaller打包配置文件
"""

import os
import sys

# 获取spec文件所在目录
spec_dir = os.path.dirname(os.path.abspath('.'))

# 需要打包的Python文件
python_files = [
    'main_ShareTransfer.py',
    'logger_config.py',
    'database_manager.py',
    'config_ShareTransfer.py',
    'ai_service_enhanced.py'
]

# 数据文件列表（如果有）
datas = []

# 隐藏导入
hiddenimports = [
    # 核心依赖
    'pandas',
    'pyodbc',
    'requests',
    'openai',
    'tenacity',
    
    # 标准库
    'concurrent.futures',
    'queue',
    'threading',
    'json',
    'os',
    'time',
    'datetime',
    'pathlib',
    'typing',
    
    # tkinter相关
    'tkinter',
    'tkinter.filedialog',
    'tkinter.ttk',
    
    # pandas相关
    'pandas._libs',
    'pandas._libs.tslibs',
    'pandas._libs.tslibs.np_datetime',
    'pandas._libs.tslibs.nattype',
    'pandas._libs.tslibs.dtypes',
]

# 排除不必要的模块
excludes = [
    # 图形界面相关（不需要）
    'matplotlib',
    'matplotlib.pyplot',
    'matplotlib.backends',
    'matplotlib.figure',
    'matplotlib.axes',
    
    # IPython相关（不需要）
    'IPython',
    'jedi',
    'parso',
    'prompt_toolkit',
    'traitlets',
    'nbformat',
    
    # PyQt相关（不需要）
    'PyQt5',
    'PyQt5.QtCore',
    'PyQt5.QtGui',
    'PyQt5.QtWidgets',
    'PyQt5.sip',
    
    # 其他不需要的依赖
    'jsonschema',
    'lark',
    'wcwidth',
    'pygments',
    'zmq',
    'pycparser',
    
    # setuptools相关（不需要）
    'setuptools',
    'pkg_resources',
    'wheel',
    'importlib_metadata',
    'more_itertools',
    'jaraco.text',
    'jaraco.functools',
    'jaraco.context',
    'backports.tarfile',
    'platformdirs',
    'backports',
    'zipp',
    
    # 测试相关
    'pytest',
    'unittest',
    'doctest',
    
    # 开发工具
    'pylint',
    'flake8',
    'black',
]

# 分析阶段
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
    optimize=2,  # 启用字节码优化
)

# 去除重复的Python模块
pyz = PYZ(a.pure, a.zipped_data, cipher=None)

# 可执行文件配置
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='股东股权变动AI比对系统 V1.1',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,  # Windows不需要strip工具
    upx=True,  # 使用UPX压缩
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # 显示控制台窗口
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # 如果有图标文件，可以在这里指定
)
