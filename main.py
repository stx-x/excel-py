import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import numpy as np
from typing import List, Dict, Tuple, Optional

class HTMLLogger:
    def __init__(self, log_file: Path):
        """HTML格式日志记录器"""
        self.log_file = log_file
        self.content_buffer = []
        self.init_html()

    def init_html(self):
        """初始化HTML文档"""
        html_header = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Excel文件处理日志</title>
    <style>
        body { font-family: 'Microsoft YaHei', Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; text-align: center; }
        .section { margin: 20px 0; padding: 15px; border-left: 4px solid #667eea; background: #f8f9ff; border-radius: 0 5px 5px 0; }
        .folder-summary { background: #e8f5e8; border-left-color: #28a745; padding: 15px; margin: 10px 0; border-radius: 5px; }
        .success { color: #28a745; font-weight: bold; }
        .warning { color: #ffc107; font-weight: bold; }
        .error { color: #dc3545; font-weight: bold; }
        .info { color: #17a2b8; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; margin: 10px 0; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; font-weight: bold; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
        .stats-card { background: #fff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; text-align: center; }
        .stats-number { font-size: 2em; font-weight: bold; color: #667eea; }
        .progress-bar { background: #e0e0e0; border-radius: 10px; overflow: hidden; margin: 10px 0; }
        .progress-fill { height: 20px; background: linear-gradient(90deg, #28a745, #20c997); color: white; text-align: center; line-height: 20px; font-size: 12px; }
        .emoji { font-size: 1.2em; }
        .timestamp { color: #ff7; font-size: 0.9em; }
        .folder-header { background: #667eea; color: white; padding: 15px; border-radius: 5px; margin: 20px 0 10px 0; }
        .processing-details { background: #f8f9fa; border-radius: 5px; padding: 10px; margin: 10px 0; }
    </style>
</head>
<body>
    <div class="container">
"""
        self.content_buffer.append(html_header)

    def add_header(self, title: str):
        """添加页面标题"""
        header_html = f"""
        <div class="header">
            <h1>📊 {title}</h1>
            <div class="timestamp">处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
        </div>
"""
        self.content_buffer.append(header_html)

    def add_section(self, title: str, content: str, css_class: str = "section"):
        """添加章节"""
        section_html = f"""
        <div class="{css_class}">
            <h3>{title}</h3>
            <div>{content}</div>
        </div>
"""
        self.content_buffer.append(section_html)

    def add_folder_summary(self, folder_name: str, summary_data: dict):
        """添加文件夹总结信息"""
        summary_html = f"""
        <div class="folder-header">
            <h2>📁 文件夹: {folder_name}</h2>
        </div>
        <div class="folder-summary">
            <h4>📊 扫描总结</h4>
            <div class="stats-grid">
                <div class="stats-card">
                    <div class="stats-number">{summary_data.get('excel_files', 0)}</div>
                    <div>Excel文件</div>
                </div>
                <div class="stats-card">
                    <div class="stats-number">{summary_data.get('total_sheets', 0)}</div>
                    <div>工作表总数</div>
                </div>
                <div class="stats-card">
                    <div class="stats-number success">{summary_data.get('processed_sheets', 0)}</div>
                    <div>成功处理</div>
                </div>
                <div class="stats-card">
                    <div class="stats-number warning">{summary_data.get('skipped_sheets', 0)}</div>
                    <div>跳过处理</div>
                </div>
                <div class="stats-card">
                    <div class="stats-number error">{summary_data.get('error_sheets', 0)}</div>
                    <div>处理错误</div>
                </div>
            </div>
            <div class="progress-bar">
                <div class="progress-fill" style="width: {summary_data.get('success_rate', 0)}%">
                    成功率: {summary_data.get('success_rate', 0):.1f}%
                </div>
            </div>
        </div>
"""
        self.content_buffer.append(summary_html)

    def add_table(self, headers: list, rows: list, table_class: str = ""):
        """添加表格"""
        if not rows:
            return

        table_html = f'<table class="{table_class}">\n<thead><tr>'
        for header in headers:
            table_html += f'<th>{header}</th>'
        table_html += '</tr></thead>\n<tbody>'

        for row in rows:
            table_html += '<tr>'
            for cell in row:
                table_html += f'<td>{cell}</td>'
            table_html += '</tr>'

        table_html += '</tbody></table>'
        self.content_buffer.append(table_html)

    def add_processing_detail(self, file_name: str, sheets_info: list):
        """添加文件处理详情"""
        detail_html = f"""
        <div class="processing-details">
            <h5>📄 {file_name}</h5>
            <ul>
"""
        for sheet_info in sheets_info:
            status_class = "success" if sheet_info['status'] == '成功处理' else "warning" if sheet_info['status'] == '跳过' else "error"
            emoji = "✅" if sheet_info['status'] == '成功处理' else "⚠️" if sheet_info['status'] == '跳过' else "❌"

            detail_html += f"""
                <li class="{status_class}">
                    <span class="emoji">{emoji}</span>
                    <strong>{sheet_info['sheet_name']}</strong> - {sheet_info['status']}
                    {f" ({sheet_info['processed_rows']}行, {sheet_info['columns_count']}列)" if sheet_info['status'] == '成功处理' else ""}
                    <br><small>原因: {sheet_info['reason']}</small>
                </li>
"""

        detail_html += """
            </ul>
        </div>
"""
        self.content_buffer.append(detail_html)

    def write_to_file(self):
        """将内容写入HTML文件"""
        # 添加HTML结尾
        html_footer = """
    </div>
    <script>
        // 添加一些交互效果
        document.querySelectorAll('.stats-card').forEach(card => {
            card.addEventListener('mouseenter', function() {
                this.style.transform = 'translateY(-2px)';
                this.style.boxShadow = '0 4px 15px rgba(0,0,0,0.1)';
            });
            card.addEventListener('mouseleave', function() {
                this.style.transform = 'translateY(0)';
                this.style.boxShadow = 'none';
            });
        });
    </script>
</body>
</html>
"""
        self.content_buffer.append(html_footer)

        # 写入文件
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write(''.join(self.content_buffer))

class ExcelProcessor:
    def __init__(self, source_folder: str = None, output_folder: str = None):
        """
        Excel文件批量处理器

        Args:
            source_folder: 源文件夹路径，如果为None则需要用户输入
            output_folder: 输出文件夹路径，默认为桌面/excel处理29
        """
        self.source_folder = source_folder

        # 设置输出路径
        desktop_path = Path.home() / "Desktop"
        self.output_folder = Path(output_folder) if output_folder else desktop_path / "excel处理29"
        self.output_folder.mkdir(exist_ok=True)

        # 设置日志
        self.setup_logging()

        # 存储处理结果
        self.all_data = []
        self.processing_summary = {
            'folders_processed': 0,
            'files_processed': 0,
            'sheets_processed': 0,
            'sheets_with_id_column': 0,
            'total_rows': 0
        }

        # 详细处理记录
        self.processing_details = {
            'folders': {},  # 改为字典，存储每个文件夹的详细信息
            'processed_sheets': [],
            'skipped_sheets': [],
            'error_sheets': [],
            'empty_sheets': []
        }

        # HTML日志记录器
        self.html_logger = HTMLLogger(self.output_folder / "处理日志.html")

    def setup_logging(self):
        """设置日志配置"""
        log_file = self.output_folder / "处理日志.txt"

        # 创建logger
        self.logger = logging.getLogger('ExcelProcessor')
        self.logger.setLevel(logging.INFO)

        # 清除之前的处理器
        self.logger.handlers.clear()

        # 文件处理器
        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 格式化
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.logger.info("🔄 " + "="*80)
        self.logger.info("📊 Excel文件批量处理开始")
        self.logger.info(f"📁 输出目录: {self.output_folder}")
        self.logger.info("🔄 " + "="*80)

    def get_source_folder(self) -> str:
        """获取源文件夹路径"""
        if not self.source_folder:
            while True:
                folder_path = input("请输入要处理的文件夹路径: ").strip()
                if os.path.exists(folder_path):
                    self.source_folder = folder_path
                    break
                else:
                    print("路径不存在，请重新输入")

        self.logger.info(f"📂 源文件夹: {self.source_folder}")
        return self.source_folder

    def find_id_column_row(self, df: pd.DataFrame) -> Optional[int]:
        """
        在前10行中查找包含'身份证号'的行

        Args:
            df: DataFrame对象

        Returns:
            包含身份证号的行索引，如果未找到返回None
        """
        search_rows = min(10, len(df))

        for row_idx in range(search_rows):
            row_data = df.iloc[row_idx]
            for cell_value in row_data:
                if pd.notna(cell_value) and '身份证号' in str(cell_value):
                    return row_idx
        return None

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        清洗数据框

        Args:
            df: 原始数据框

        Returns:
            清洗后的数据框
        """
        # 删除全空行
        df = df.dropna(how='all')

        # 删除全空列
        df = df.dropna(axis=1, how='all')

        # 重置索引
        df = df.reset_index(drop=True)

        return df

    def process_excel_file(self, file_path: Path, folder_name: str) -> Tuple[List[Dict], List[Dict]]:
        """
        处理单个Excel文件

        Args:
            file_path: Excel文件路径
            folder_name: 所在文件夹名称

        Returns:
            (处理后的数据列表, 工作表信息列表)
        """
        file_data = []
        sheets_info = []
        file_name = file_path.name

        self.logger.info(f"  📄 处理文件: {file_name}")

        try:
            # 读取所有工作表名称
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names

            self.logger.info(f"    📋 发现 {len(sheet_names)} 个工作表: {sheet_names}")

            for sheet_name in sheet_names:
                self.logger.info(f"    📝 处理工作表: {sheet_name}")
                sheet_info = {
                    'folder_name': folder_name,
                    'file_name': file_name,
                    'sheet_name': sheet_name,
                    'status': '',
                    'reason': '',
                    'original_rows': 0,
                    'processed_rows': 0,
                    'columns_count': 0
                }

                try:
                    # 读取工作表（不设置header，让程序自动识别）
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    sheet_info['original_rows'] = len(df)

                    if df.empty:
                        sheet_info['status'] = '跳过'
                        sheet_info['reason'] = '工作表为空'
                        self.processing_details['empty_sheets'].append(sheet_info)
                        self.logger.info(f"      ⚠️  工作表为空，已跳过")
                        sheets_info.append(sheet_info)
                        continue

                    # 查找身份证号列所在行
                    id_row_idx = self.find_id_column_row(df)

                    if id_row_idx is not None:
                        self.logger.info(f"      ✅ 找到身份证号列在第 {id_row_idx + 1} 行")
                        self.processing_summary['sheets_with_id_column'] += 1

                        # 设置列名
                        new_columns = df.iloc[id_row_idx].fillna('未命名列').astype(str)

                        # 获取数据部分（表头之后的数据）
                        data_df = df.iloc[id_row_idx + 1:].copy()
                        data_df.columns = new_columns

                        # 清洗数据
                        data_df = self.clean_dataframe(data_df)

                        if not data_df.empty:
                            # 添加来源信息
                            data_df['文件名'] = file_name
                            data_df['工作表名'] = sheet_name
                            data_df['文件夹名'] = folder_name

                            sheet_info['status'] = '成功处理'
                            sheet_info['processed_rows'] = len(data_df)
                            sheet_info['columns_count'] = len(data_df.columns)
                            sheet_info['reason'] = f'包含身份证号列，处理{len(data_df)}行数据'

                            file_data.append({
                                'data': data_df,
                                'file_name': file_name,
                                'sheet_name': sheet_name,
                                'folder_name': folder_name,
                                'original_rows': len(df),
                                'processed_rows': len(data_df),
                                'columns': list(data_df.columns)
                            })

                            self.processing_details['processed_sheets'].append(sheet_info)
                            self.logger.info(f"      🎉 成功处理，数据行数: {len(data_df)}，列数: {len(data_df.columns)}")
                            self.processing_summary['total_rows'] += len(data_df)
                        else:
                            sheet_info['status'] = '跳过'
                            sheet_info['reason'] = '清洗后数据为空'
                            self.processing_details['skipped_sheets'].append(sheet_info)
                            self.logger.info(f"      ⚠️  清洗后数据为空，已跳过")
                    else:
                        sheet_info['status'] = '跳过'
                        sheet_info['reason'] = '未找到身份证号列'
                        self.processing_details['skipped_sheets'].append(sheet_info)
                        self.logger.info(f"      ❌ 未找到身份证号列，已跳过")

                    self.processing_summary['sheets_processed'] += 1
                    sheets_info.append(sheet_info)

                except Exception as e:
                    sheet_info['status'] = '错误'
                    sheet_info['reason'] = str(e)
                    self.processing_details['error_sheets'].append(sheet_info)
                    self.logger.error(f"      💥 处理工作表出错: {str(e)}")
                    sheets_info.append(sheet_info)

            excel_file.close()
            self.processing_summary['files_processed'] += 1

        except Exception as e:
            self.logger.error(f"  处理文件 {file_name} 时出错: {str(e)}")

        return file_data, sheets_info

    def merge_all_data(self, all_data: List[Dict]) -> pd.DataFrame:
        """
        合并所有数据

        Args:
            all_data: 所有处理后的数据

        Returns:
            合并后的DataFrame
        """
        if not all_data:
            return pd.DataFrame()

        self.logger.info("🔗 开始合并所有数据...")

        # 收集所有列名（做并集）
        all_columns = set()
        for data_info in all_data:
            all_columns.update(data_info['data'].columns)

        all_columns = sorted(list(all_columns))
        self.logger.info(f"📊 合并后总列数: {len(all_columns)}")

        # 统一列结构并合并
        unified_dataframes = []
        for data_info in all_data:
            df = data_info['data'].copy()

            # 为缺失的列添加NaN
            for col in all_columns:
                if col not in df.columns:
                    df[col] = np.nan

            # 重新排序列
            df = df.reindex(columns=all_columns)
            unified_dataframes.append(df)

        # 合并所有数据
        merged_df = pd.concat(unified_dataframes, ignore_index=True)

        self.logger.info(f"✅ 合并完成，总行数: {len(merged_df)}")
        return merged_df

    def process_folders(self) -> bool:
        """
        处理所有以'优'开头的子文件夹

        Returns:
            处理是否成功
        """
        source_folder = Path(self.get_source_folder())

        if not source_folder.exists():
            self.logger.error(f"源文件夹不存在: {source_folder}")
            return False

        # 查找所有以'优'开头的子文件夹
        target_folders = [f for f in source_folder.iterdir()
                         if f.is_dir() and f.name.startswith('优')]

        if not target_folders:
            self.logger.warning("未找到以'优'开头的子文件夹")
            return False

        self.logger.info(f"🎯 找到 {len(target_folders)} 个以'优'开头的文件夹:")
        for folder in target_folders:
            self.logger.info(f"  📁 {folder.name}")

        # 初始化HTML日志
        self.html_logger.add_header("Excel文件批量处理日志")

        # 添加源文件夹信息
        source_info = f"<p><strong>📂 源文件夹:</strong> {source_folder}</p>"
        source_info += f"<p><strong>🎯 找到目标文件夹:</strong> {len(target_folders)} 个</p>"
        source_info += "<ul>"
        for folder in target_folders:
            source_info += f"<li>📁 {folder.name}</li>"
        source_info += "</ul>"
        self.html_logger.add_section("📂 扫描概览", source_info)

        # 处理每个文件夹
        all_processed_data = []

        for folder in target_folders:
            self.logger.info(f"\n📂 正在处理文件夹: {folder.name}")
            self.logger.info(f"{'='*60}")

            # 查找文件夹中的所有Excel文件
            excel_files = list(folder.glob("*.xlsx")) + list(folder.glob("*.xls"))

            if not excel_files:
                self.logger.info(f"  ⚠️  文件夹中无Excel文件")
                continue

            self.logger.info(f"  📊 找到 {len(excel_files)} 个Excel文件:")
            for excel_file in excel_files:
                self.logger.info(f"    - {excel_file.name}")

            # 处理文件夹中的所有文件，收集统计信息
            folder_processed_data = []
            folder_sheets_info = []

            for excel_file in excel_files:
                file_data, sheets_info = self.process_excel_file(excel_file, folder.name)
                folder_processed_data.extend(file_data)
                folder_sheets_info.extend(sheets_info)

            # 计算文件夹统计信息
            total_sheets = len(folder_sheets_info)
            processed_sheets = len([s for s in folder_sheets_info if s['status'] == '成功处理'])
            skipped_sheets = len([s for s in folder_sheets_info if s['status'] == '跳过'])
            error_sheets = len([s for s in folder_sheets_info if s['status'] == '错误'])
            success_rate = (processed_sheets / total_sheets * 100) if total_sheets > 0 else 0

            # 添加文件夹总结到HTML
            summary_data = {
                'excel_files': len(excel_files),
                'total_sheets': total_sheets,
                'processed_sheets': processed_sheets,
                'skipped_sheets': skipped_sheets,
                'error_sheets': error_sheets,
                'success_rate': success_rate
            }
            self.html_logger.add_folder_summary(folder.name, summary_data)

            # 按文件分组添加处理详情
            files_dict = {}
            for sheet_info in folder_sheets_info:
                file_name = sheet_info['file_name']
                if file_name not in files_dict:
                    files_dict[file_name] = []
                files_dict[file_name].append(sheet_info)

            for file_name, sheets_info in files_dict.items():
                self.html_logger.add_processing_detail(file_name, sheets_info)

            all_processed_data.extend(folder_processed_data)
            self.processing_summary['folders_processed'] += 1

        # 存储处理结果
        self.all_data = all_processed_data
        return len(all_processed_data) > 0

    def save_results(self) -> bool:
        """
        保存处理结果

        Returns:
            保存是否成功
        """
        if not self.all_data:
            self.logger.warning("没有数据需要保存")
            return False

        # 合并数据
        merged_df = self.merge_all_data(self.all_data)

        if merged_df.empty:
            self.logger.warning("合并后数据为空")
            return False

        # 保存到Excel文件
        output_file = self.output_folder / "结果.xlsx"

        try:
            merged_df.to_excel(output_file, index=False, engine='openpyxl')
            self.logger.info(f"💾 结果已保存到: {output_file}")

            # 记录处理统计信息
            self.log_summary()
            self.generate_html_summary()

            return True

        except Exception as e:
            self.logger.error(f"保存文件时出错: {str(e)}")
            return False

    def generate_html_summary(self):
        """生成HTML格式的最终摘要"""
        # 总体统计
        total_sheets = len(self.processing_details['processed_sheets']) + len(self.processing_details['skipped_sheets']) + len(self.processing_details['empty_sheets']) + len(self.processing_details['error_sheets'])
        success_rate = (len(self.processing_details['processed_sheets']) / total_sheets * 100) if total_sheets > 0 else 0

        summary_content = f"""
        <div class="stats-grid">
            <div class="stats-card">
                <div class="stats-number">{self.processing_summary['folders_processed']}</div>
                <div>处理文件夹</div>
            </div>
            <div class="stats-card">
                <div class="stats-number">{self.processing_summary['files_processed']}</div>
                <div>处理文件</div>
            </div>
            <div class="stats-card">
                <div class="stats-number success">{len(self.processing_details['processed_sheets'])}</div>
                <div>成功工作表</div>
            </div>
            <div class="stats-card">
                <div class="stats-number warning">{len(self.processing_details['skipped_sheets']) + len(self.processing_details['empty_sheets'])}</div>
                <div>跳过工作表</div>
            </div>
            <div class="stats-card">
                <div class="stats-number error">{len(self.processing_details['error_sheets'])}</div>
                <div>错误工作表</div>
            </div>
            <div class="stats-card">
                <div class="stats-number info">{self.processing_summary['total_rows']}</div>
                <div>最终数据行</div>
            </div>
        </div>
        <div class="progress-bar">
            <div class="progress-fill" style="width: {success_rate}%">
                总成功率: {success_rate:.1f}%
            </div>
        </div>
        """

        self.html_logger.add_section("📊 最终统计摘要", summary_content)

        # 成功处理的工作表
        if self.processing_details['processed_sheets']:
            headers = ['文件夹', '文件名', '工作表名', '原始行数', '处理后行数', '列数']
            rows = []
            for sheet in self.processing_details['processed_sheets']:
                rows.append([
                    sheet['folder_name'],
                    sheet['file_name'],
                    sheet['sheet_name'],
                    sheet['original_rows'],
                    sheet['processed_rows'],
                    sheet['columns_count']
                ])

            self.html_logger.add_section("✅ 成功处理的工作表", "", "section")
            self.html_logger.add_table(headers, rows)

        # 跳过的工作表
        skipped_all = self.processing_details['skipped_sheets'] + self.processing_details['empty_sheets']
        if skipped_all:
            headers = ['文件夹', '文件名', '工作表名', '跳过原因']
            rows = []
            for sheet in skipped_all:
                rows.append([
                    sheet['folder_name'],
                    sheet['file_name'],
                    sheet['sheet_name'],
                    sheet['reason']
                ])

            self.html_logger.add_section("⚠️ 跳过的工作表", "", "section")
            self.html_logger.add_table(headers, rows)

        # 错误的工作表
        if self.processing_details['error_sheets']:
            headers = ['文件夹', '文件名', '工作表名', '错误信息']
            rows = []
            for sheet in self.processing_details['error_sheets']:
                rows.append([
                    sheet['folder_name'],
                    sheet['file_name'],
                    sheet['sheet_name'],
                    sheet['reason']
                ])

            self.html_logger.add_section("💥 处理错误的工作表", "", "section")
            self.html_logger.add_table(headers, rows)

        # 写入HTML文件
        self.html_logger.write_to_file()

    def log_summary(self):
        """记录处理摘要"""
        self.logger.info("\n" + "🎯 " + "="*80)
        self.logger.info("📊 最终处理摘要")
        self.logger.info("🎯 " + "="*80)

        # 总体统计
        self.logger.info("📈 总体统计:")
        self.logger.info(f"   📁 扫描文件夹数: {self.processing_summary['folders_processed']}")
        self.logger.info(f"   📄 处理Excel文件数: {self.processing_summary['files_processed']}")
        self.logger.info(f"   📋 总工作表数: {self.processing_summary['sheets_processed']}")
        self.logger.info(f"   ✅ 成功处理工作表数: {len(self.processing_details['processed_sheets'])}")
        self.logger.info(f"   ❌ 跳过工作表数: {len(self.processing_details['skipped_sheets']) + len(self.processing_details['empty_sheets'])}")
        self.logger.info(f"   💥 错误工作表数: {len(self.processing_details['error_sheets'])}")
        self.logger.info(f"   📊 最终数据行数: {self.processing_summary['total_rows']}")

        # 处理成功率
        total_sheets = len(self.processing_details['processed_sheets']) + len(self.processing_details['skipped_sheets']) + len(self.processing_details['empty_sheets']) + len(self.processing_details['error_sheets'])
        success_rate = (len(self.processing_details['processed_sheets']) / total_sheets * 100) if total_sheets > 0 else 0

        self.logger.info(f"\n📊 处理成功率: {success_rate:.1f}% ({len(self.processing_details['processed_sheets'])}/{total_sheets})")

        self.logger.info("\n🎯 " + "="*80)
        self.logger.info("🎉 处理完成!")
        self.logger.info("🎯 " + "="*80)

    def run(self):
        """运行主程序"""
        try:
            print("Excel文件批量处理程序")
            print("="*40)

            # 处理文件夹
            if not self.process_folders():
                print("处理失败或无数据需要处理")
                return

            # 保存结果
            if self.save_results():
                print(f"处理完成! 结果已保存到: {self.output_folder}")
                print(f"文本日志: {self.output_folder / '处理日志.txt'}")
                print(f"HTML日志: {self.output_folder / '处理日志.html'}")
            else:
                print("保存结果时出错")

        except KeyboardInterrupt:
            self.logger.info("用户中断程序")
            print("\n程序被用户中断")
        except Exception as e:
            self.logger.error(f"程序运行时出现未预期的错误: {str(e)}")
            print(f"程序出错: {str(e)}")


def main():
    """主函数"""
    # 可以在这里指定源文件夹，如果为None则会提示用户输入
    processor = ExcelProcessor(source_folder=None)
    processor.run()


if __name__ == "__main__":
    main()
