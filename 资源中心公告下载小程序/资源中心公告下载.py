import sys
import datetime
import os
import re
import pandas as pd
import pyodbc
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QTextEdit, QLineEdit, QPushButton,
                             QLabel, QFileDialog, QMessageBox, QProgressBar,
                             QGroupBox, QPlainTextEdit)
from PyQt5.QtCore import pyqtSignal, QObject, Qt, QEvent
from PyQt5.QtGui import QFont, QIcon

# 数据库连接配置
SERVER = '10.102.25.11,8080'
USERNAME = 'WebResourceNew_Read'
PASSWORD = 'New_45ted'
DRIVER = 'ODBC Driver 17 for SQL Server'


class DownloadSignals(QObject):
    progress_update = pyqtSignal(int, int, int)  # 当前进度, 成功数, 失败数
    log_message = pyqtSignal(str)
    finished = pyqtSignal(int, int, list)  # 成功数, 失败数, 失败文件列表
    file_downloaded = pyqtSignal(str)


def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


class ResourceDownloader(QMainWindow):
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.is_downloading = False
        self.download_thread = None
        self.download_signals = DownloadSignals()
        self.processed_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.total_files = 0
        self.failed_files = []
        self.executor = None
        self.start_time = None  # 添加开始时间变量

        ico_path = resource_path(os.path.join("云下载.ico"))
        self.setWindowIcon(QIcon(ico_path))
        QApplication.setWindowIcon(QIcon(ico_path))

        # 连接信号和槽
        self.download_signals.progress_update.connect(self.update_progress)
        self.download_signals.log_message.connect(self.append_log)
        self.download_signals.finished.connect(self.download_finished)
        self.download_signals.file_downloaded.connect(self.log_file_downloaded)

    def init_ui(self):
        self.setWindowTitle("资源中心公告下载工具")
        self.setGeometry(100, 100, 600, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Hashcode显示区域 - 使用QVBoxLayout替代QGroupBox去除边框
        hash_label = QLabel("MD5列表 (从Excel文件读取或手动输入)")
        hash_label.setStyleSheet("margin-bottom: 5px;")
        layout.addWidget(hash_label)
        
        self.hash_edit = QPlainTextEdit()
        self.hash_edit.setPlaceholderText("MD5将在此显示，也可手动输入MD5，每行一个...")

        # 设置等宽字体并禁用自动换行
        font = QFont("Courier New", 9)
        self.hash_edit.setFont(font)
        self.hash_edit.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.hash_edit.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        layout.addWidget(self.hash_edit)

        # Excel文件上传区域 - 去除标题
        # 文件选择区域
        file_select_layout = QHBoxLayout()
        self.file_path_edit = QLineEdit()
        self.browse_file_btn = QPushButton("上传")
        self.browse_file_btn.clicked.connect(self.browse_excel_file)
        file_select_layout.addWidget(self.file_path_edit)
        file_select_layout.addWidget(self.browse_file_btn)

        layout.addLayout(file_select_layout)

        # 保存路径区域 - 去除标题
        path_layout = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("请选择保存路径...")
        self.path_edit.setText(os.path.join(os.path.expanduser("~"), "Desktop",
                                            datetime.date.today().strftime('%Y-%m-%d')))
        self.browse_path_btn = QPushButton("保存")
        self.browse_path_btn.clicked.connect(self.browse_path)
        path_layout.addWidget(self.path_edit)
        path_layout.addWidget(self.browse_path_btn)
        layout.addLayout(path_layout)

        # 按钮区域
        btn_layout = QHBoxLayout()
        self.download_btn = QPushButton("开始下载")
        self.download_btn.clicked.connect(self.start_download)

        # 添加停止按钮
        self.stop_btn = QPushButton("停止下载")
        self.stop_btn.clicked.connect(self.stop_download)
        self.stop_btn.setEnabled(False)

        btn_layout.addWidget(self.download_btn)
        btn_layout.addWidget(self.stop_btn)
        layout.addLayout(btn_layout)

        # 进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)

        # 日志区域 - 使用QVBoxLayout替代QGroupBox去除边框
        log_label = QLabel("下载日志")
        log_label.setStyleSheet("margin-top: 10px; margin-bottom: 5px;")
        layout.addWidget(log_label)
        
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        layout.addWidget(self.log_edit)

        # 状态信息
        self.status_label = QLabel("就绪")
        layout.addWidget(self.status_label)

    def browse_excel_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "选择Excel文件",
            "",
            "Excel Files (*.xlsx *.xls);;All Files (*)"
        )
        if file_path:
            self.file_path_edit.setText(file_path)
            self.load_excel_file(file_path)

    def load_excel_file(self, file_path):
        try:
            df = pd.read_excel(file_path)
            hashcode_columns = [col for col in df.columns if
                                any(keyword in col.upper() for keyword in ['HASHCODE', 'MD5'])]

            if not hashcode_columns:
                column_name = df.columns[0]
                self.log_edit.append(f"警告: 未找到包含'MD5'的列，默认使用第一列: {column_name}")
            else:
                column_name = hashcode_columns[0]

            hashcodes = df[column_name].dropna().astype(str).unique().tolist()
            hashcodes = [h.strip() for h in hashcodes if h.strip()]

            if not hashcodes:
                QMessageBox.warning(self, "警告", "Excel文件中没有有效的MD5")
                return

            self.hash_edit.setPlainText('\n'.join(hashcodes))
            self.log_edit.append(f"从Excel文件加载了 {len(hashcodes)} 个MD5 (列名: {column_name})")

        except Exception as e:
            QMessageBox.critical(self, "错误", f"读取Excel文件时出错: {str(e)}")

    def browse_path(self):
        path = QFileDialog.getExistingDirectory(self, "选择保存目录")
        if path:
            self.path_edit.setText(path)

    def start_download(self):
        hashcodes_text = self.hash_edit.toPlainText().strip()
        if not hashcodes_text:
            QMessageBox.warning(self, "警告", "请先上传Excel文件或输入MD5")
            return

        hashcodes = [h.strip() for h in hashcodes_text.split('\n') if h.strip()]
        if not hashcodes:
            QMessageBox.warning(self, "警告", "没有有效的MD5")
            return

        save_path = self.path_edit.text()
        if not save_path:
            QMessageBox.warning(self, "警告", "请先选择保存路径")
            return

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        # 记录开始时间
        self.start_time = datetime.datetime.now()
        self.log_edit.append(f"开始批量下载 {len(hashcodes)} 个文件...")

        self.is_downloading = True
        self.download_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_bar.setMaximum(len(hashcodes))
        self.progress_bar.setValue(0)
        self.progress_bar.setVisible(True)

        # 重置计数器
        self.processed_count = 0
        self.success_count = 0
        self.fail_count = 0
        self.total_files = len(hashcodes)
        self.failed_files = []

        self.download_thread = threading.Thread(
            target=self.download_batch_files_thread,
            args=(hashcodes, save_path)
        )
        self.download_thread.daemon = True
        self.download_thread.start()

    def stop_download(self):
        self.is_downloading = False
        if self.executor:
            self.executor.shutdown(wait=False)
        self.append_log("用户请求停止下载...")
        self.status_label.setText("下载已停止")
        self.download_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

    def download_batch_files_thread(self, hashcodes, save_path):
        sql_template = '''
            SELECT C.GPDM,CONVERT(DATE,A.XXFBRQ) XXFBRQ,A.XXBT,B.MS,A.HASHCODE
            FROM [10.101.0.212].JYPRIME.dbo.usrGSGGYWFWB A
            JOIN [10.101.0.212].JYPRIME.dbo.usrXTCLB B
                ON A.WJGS = B.DM AND B.LB = '1309'
            JOIN [10.101.0.212].JYPRIME.dbo.usrZQZB C
                ON C.INBBM = A.INBBM AND C.ZQSC IN (83, 90, 18) AND C.ZQLB IN (1, 2, 41)
            WHERE A.HASHCODE = '{hashcode}'
        '''

        try:
            # 使用线程池进行并发下载（2个线程）
            self.executor = ThreadPoolExecutor(max_workers=5)

            # 分批处理，避免内存溢出
            batch_size = 10
            for i in range(0, len(hashcodes), batch_size):
                if not self.is_downloading:
                    break

                batch = hashcodes[i:i + batch_size]
                futures = []

                for hashcode in batch:
                    if not self.is_downloading:
                        break
                    future = self.executor.submit(self.process_single_hashcode, hashcode, sql_template, save_path)
                    futures.append((future, hashcode))

                # 等待当前批次完成
                for future, hashcode in futures:
                    if not self.is_downloading:
                        break
                    try:
                        result = future.result(timeout=120)  # 2分钟超时
                        if result:
                            self.success_count += 1
                        else:
                            self.fail_count += 1
                            self.failed_files.append(hashcode)
                    except Exception as e:
                        self.download_signals.log_message.emit(f"处理MD5 {hashcode} 时出错: {str(e)}")
                        self.fail_count += 1
                        self.failed_files.append(hashcode)

                    self.processed_count += 1
                    # 使用QMetaObject.invokeMethod确保线程安全
                    QApplication.instance().postEvent(self,
                                                      ProgressEvent(self.processed_count, self.success_count,
                                                                    self.fail_count, self.failed_files))

        except Exception as e:
            self.download_signals.log_message.emit(f"下载过程中发生错误: {str(e)}")
        finally:
            if self.executor:
                self.executor.shutdown(wait=True)
            self.download_signals.finished.emit(self.success_count, self.fail_count, self.failed_files)

    def process_single_hashcode(self, hashcode, sql_template, save_path):
        """处理单个hashcode的查询和下载"""
        if not self.is_downloading:
            return False

        try:
            sql_query = sql_template.format(hashcode=hashcode)
            data_list = self.query_data(sql_query)

            if data_list and len(data_list) > 0:
                # 由于一个MD5只对应一个文件，直接下载第一个结果
                self.download_single_file(data_list[0], save_path)
                return True
            else:
                self.download_signals.log_message.emit(f"未找到MD5 {hashcode} 对应的数据")
                return False

        except Exception as e:
            self.download_signals.log_message.emit(f"处理MD5 {hashcode} 时出错: {str(e)}")
            return False

    def query_data(self, sql_query):
        result_list = []
        try:
            conn = pyodbc.connect(SERVER=SERVER, UID=USERNAME, PWD=PASSWORD, DRIVER=DRIVER)
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            conn.close()

            for item in result:
                result_list.append(item)

        except pyodbc.Error as e:
            raise Exception(f"数据库查询错误: {e}")

        return result_list

    def download_single_file(self, app_id, save_path):
        """下载单个文件"""
        if not self.is_downloading:
            return

        try:
            url_template = 'http://10.6.1.131/rfApi/file/downloadWithAppId/{appId}?appId=rc-as'
            download_url = url_template.format(appId=app_id[4])

            # 添加请求头，模拟浏览器行为
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(download_url, headers=headers, timeout=60, stream=True)

            hz = str(app_id[3])
            rq = str(app_id[1])
            bt = app_id[2]
            filename = f"{app_id[0]}-{rq}-{bt}.{hz}"
            filename = re.sub(r'[\\/*?:"<>|]', '', filename)
            file_path = os.path.join(save_path, filename)

            # 使用流式下载，避免大文件内存问题
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not self.is_downloading:
                        break
                    if chunk:
                        f.write(chunk)

            if self.is_downloading:  # 只有在未停止的情况下才发送成功消息
                self.download_signals.file_downloaded.emit(f"已下载: {filename}")

        except Exception as e:
            if self.is_downloading:  # 只有在未停止的情况下才发送错误消息
                filename_str = filename if 'filename' in locals() else '未知文件'
                self.download_signals.log_message.emit(f"下载失败: {filename_str} - {str(e)}")
            raise

    def event(self, event):
        """处理自定义进度更新事件"""
        if isinstance(event, ProgressEvent):
            self.update_progress(event.current, event.success, event.fail)
            return True
        return super().event(event)

    def update_progress(self, current, success, fail):
        """更新进度条和状态标签"""
        self.progress_bar.setValue(current)
        self.status_label.setText(f"进度: {current}/{self.total_files} (成功: {success}, 失败: {fail})")

    def append_log(self, message):
        self.log_edit.append(message)

    def log_file_downloaded(self, message):
        self.log_edit.append(message)

    def download_finished(self, success_count, fail_count, failed_files):
        self.progress_bar.setVisible(False)
        self.download_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.is_downloading = False
        self.executor = None

        # 计算并显示总用时
        if self.start_time:
            end_time = datetime.datetime.now()
            duration = end_time - self.start_time
            duration_str = str(duration).split('.')[0]  # 去掉微秒部分
        else:
            duration_str = "未知"

        if self.processed_count < self.total_files:
            self.log_edit.append(
                f"下载已停止! 已处理: {self.processed_count}/{self.total_files}, 成功: {success_count}, 失败: {fail_count}, 用时: {duration_str}")
        else:
            self.log_edit.append(f"批量下载完成! 成功: {success_count}, 失败: {fail_count}, 用时: {duration_str}")
            if fail_count > 0 and failed_files:
                self.log_edit.append("失败的文件MD5:")
                for failed_file in failed_files:
                    self.log_edit.append(f"{failed_file}")
                self.log_edit.append("")


# 自定义事件用于线程安全的进度更新
class ProgressEvent(QEvent):
    def __init__(self, current: object, success: object, fail: object, failed_files: list = None) -> None:
        super().__init__(QEvent.User)
        self.current = current
        self.success = success
        self.fail = fail
        self.failed_files = failed_files if failed_files is not None else []


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = ResourceDownloader()
    window.show()
    sys.exit(app.exec_())
