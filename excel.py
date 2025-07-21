"""
Excel数据处理器 - 大师级实现
优雅、精简、高性能的Excel文件批量处理工具
"""

from pathlib import Path
from typing import Iterator, Optional, Dict, List, Any, Tuple, Set
from dataclasses import dataclass
from functools import wraps
from collections import defaultdict
import logging

import pandas as pd

# 配置详细日志
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ProcessResult:
    """处理结果数据类"""
    data: pd.DataFrame
    files_processed: int
    sheets_processed: int
    total_rows: int
    column_sources: Dict[str, Set[Tuple[str, str]]]
    processing_stats: Dict[str, Any]
    scan_summary: Dict[str, Any]


def handle_errors(func):
    """错误处理装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"❌ {func.__name__}: {e}")
            return None
    return wrapper


def find_target_row(df: pd.DataFrame, target: str = "身份证号") -> Optional[int]:
    """在前10行中查找包含目标字符串的行"""
    for i in range(min(10, len(df))):
        if df.iloc[i].astype(str).str.contains(target, na=False).any():
            return i
    return None


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """数据清洗 - 链式操作"""
    return (df
            .dropna(how='all')  # 删除空行
            .dropna(axis=1, how='all')  # 删除空列
            .loc[:, ~df.apply(lambda x: x.astype(str).str.strip().eq('').all())]  # 删除空字符串列
            .pipe(lambda x: x[~x.apply(lambda row: row.astype(str).str.strip().eq('').all(), axis=1)])  # 删除空字符串行
            .reset_index(drop=True))


def create_headers(row_data: pd.Series) -> List[str]:
    """创建唯一列名"""
    headers = []
    for i, val in enumerate(row_data):
        name = str(val) if pd.notna(val) else f"未命名_{i}"
        # 处理重复列名
        original = name
        counter = 1
        while name in headers:
            name = f"{original}_{counter}"
            counter += 1
        headers.append(name)
    return headers


@handle_errors
def process_sheet(file_path: Path, sheet_name: str) -> Optional[pd.DataFrame]:
    """处理单个工作表"""
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    if df.empty:
        return None

    # 查找目标行
    target_row = find_target_row(df)
    if target_row is None:
        return None

    # 重构数据
    headers = create_headers(df.iloc[target_row])
    data = df.iloc[target_row + 1:].copy()
    data.columns = headers

    # 清洗并添加元信息
    return (clean_data(data)
            .assign(文件名=file_path.name, 工作表名=sheet_name)
            .pipe(lambda x: x if not x.empty else None))


def process_file(file_path: Path) -> Tuple[List[pd.DataFrame], Dict[str, Any]]:
    """处理单个文件的所有工作表"""
    relative_path = file_path.relative_to(file_path.parent.parent)
    logger.info(f"\n📂 处理文件: {relative_path}")
    logger.info("─" * 60)

    file_stats = {
        'file_name': file_path.name,
        'relative_path': str(relative_path),
        'sheets_processed': 0,
        'sheets_skipped': 0,
        'total_data_rows': 0,
        'sheet_details': []
    }

    try:
        with pd.ExcelFile(file_path) as excel_file:
            sheet_names = excel_file.sheet_names
            file_stats['total_sheets'] = len(sheet_names)

            logger.info(f"   📊 发现 {len(sheet_names)} 个工作表: {', '.join(sheet_names)}")

            processed_dfs = []
            for sheet_idx, sheet_name in enumerate(sheet_names, 1):
                check_msg = f"   └─ [{sheet_idx}/{len(sheet_names)}] 🔍 检查工作表: '{sheet_name}' "

                result = process_sheet(file_path, sheet_name)

                sheet_detail = {
                    'sheet_name': sheet_name,
                    'has_target': False,
                    'data_rows': 0,
                    'columns_count': 0
                }

                if result is not None:
                    processed_dfs.append(result)
                    file_stats['sheets_processed'] += 1
                    file_stats['total_data_rows'] += len(result)

                    data_columns = [col for col in result.columns if col not in ['文件名', '工作表名']]
                    sheet_detail.update({
                        'has_target': True,
                        'data_rows': len(result),
                        'columns_count': len(data_columns)
                    })

                    logger.info(check_msg + f"✅ (找到目标字符串，获得 {len(result)} 行数据)")
                else:
                    file_stats['sheets_skipped'] += 1
                    logger.info(check_msg + "❌ (未找到包含'身份证号'的行)")

                file_stats['sheet_details'].append(sheet_detail)

            if file_stats['sheets_processed'] > 0:
                logger.info(f"   📈 文件汇总: 成功处理 {file_stats['sheets_processed']} 个工作表，共 {file_stats['total_data_rows']} 行数据")
            else:
                logger.info("   ⚠️  文件汇总: 该文件中没有找到任何有效数据")

            return processed_dfs, file_stats

    except Exception as e:
        logger.warning(f"   ❌ 处理文件时出错: {e}")
        error_info = {
            'file_name': file_path.name,
            'relative_path': str(relative_path),
            'error': str(e)
        }
        return [], {'error': error_info, **file_stats}


def scan_directory_structure(folder_path: Path) -> Tuple[List[Path], Dict[str, Any]]:
    """扫描目录结构，获取完整统计信息"""
    logger.info(f"📁 开始扫描目录结构: {folder_path}")

    # 扫描所有子文件夹
    subdirs = [d for d in folder_path.iterdir() if d.is_dir()]

    # 扫描所有Excel文件（不限制文件名）
    all_excel_files = list(folder_path.glob("*/*.xlsx")) + list(folder_path.glob("*/*.xls"))

    # 匹配目标文件（以"优"开头的xlsx文件）
    target_files = list(folder_path.glob("*/优*.xlsx")) + list(folder_path.glob("*/优*.xls"))

    # 按子文件夹分组统计
    files_by_subdir = defaultdict(lambda: {'all': [], 'target': []})
    for file_path in all_excel_files:
        subdir_name = file_path.parent.name
        files_by_subdir[subdir_name]['all'].append(file_path)
        if file_path.name.startswith('优') and (file_path.suffix == '.xlsx' or file_path.suffix == '.xls'):
            files_by_subdir[subdir_name]['target'].append(file_path)

    scan_summary = {
        'total_subdirs': len(subdirs),
        'subdirs_with_excel': len(files_by_subdir),
        'all_excel_files': len(all_excel_files),
        'target_excel_files': len(target_files),
        'subdirs_detail': dict(files_by_subdir),
        'subdir_names': [d.name for d in subdirs]
    }

    # 详细日志输出
    logger.info(f"📊 目录扫描结果:")
    logger.info(f"   • 总子文件夹数: {scan_summary['total_subdirs']}")
    logger.info(f"   • 包含Excel的子文件夹: {scan_summary['subdirs_with_excel']}")
    logger.info(f"   • 所有Excel文件: {scan_summary['all_excel_files']} 个")
    logger.info(f"   • 目标Excel文件: {scan_summary['target_excel_files']} 个")

    if scan_summary['subdirs_with_excel'] > 0:
        logger.info(f"📋 各子文件夹Excel文件分布:")
        for subdir, files_info in files_by_subdir.items():
            all_count = len(files_info['all'])
            target_count = len(files_info['target'])
            logger.info(f"   • {subdir}: {all_count} 个Excel文件 (其中 {target_count} 个目标文件)")

    if scan_summary['all_excel_files'] > scan_summary['target_excel_files']:
        missed_files = scan_summary['all_excel_files'] - scan_summary['target_excel_files']
        logger.info(f"⚠️  发现 {missed_files} 个Excel文件不匹配处理条件（非'优'开头的xlsx文件）")

    return target_files, scan_summary

def get_excel_files(folder_path: Path) -> Tuple[List[Path], Dict[str, Any]]:
    """获取目标Excel文件和扫描摘要"""
    logger.info("🔄 搜索Excel文件...")
    target_files, scan_summary = scan_directory_structure(folder_path)

    if not target_files:
        raise FileNotFoundError("❌ 在指定路径的子文件夹中没有找到以'优'字开头的xlsx文件")

    logger.info(f"✅ 找到 {len(target_files)} 个匹配的目标文件:")
    for i, file_path in enumerate(target_files, 1):
        relative_path = file_path.relative_to(folder_path)
        logger.info(f"   {i:2d}. {relative_path}")

    logger.info("✅ 搜索Excel文件 完成")
    return target_files, scan_summary


def unify_columns(dataframes: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """统一列结构"""
    if not dataframes:
        return []

    # 获取所有列的并集
    all_columns = set()
    for df in dataframes:
        all_columns.update(col for col in df.columns if col not in ['文件名', '工作表名'])

    unified_columns = sorted(all_columns) + ['文件名', '工作表名']

    # 统一所有DataFrame的列
    logger.info(f"📋 统一列结构: 共 {len(unified_columns) - 2} 个数据列")
    return [df.reindex(columns=unified_columns, fill_value=None) for df in dataframes]


def collect_column_sources(dataframes: List[pd.DataFrame]) -> Dict[str, Set[Tuple[str, str]]]:
    """收集列来源信息"""
    column_sources = defaultdict(set)

    for df in dataframes:
        data_columns = [col for col in df.columns if col not in ['文件名', '工作表名']]
        if not df.empty:
            file_name = df['文件名'].iloc[0]
            sheet_name = df['工作表名'].iloc[0]

            for col in data_columns:
                column_sources[col].add((file_name, sheet_name))

    return column_sources


def log_column_sources(column_sources: Dict[str, Set[Tuple[str, str]]]) -> None:
    """记录列来源信息"""
    data_columns = [col for col in column_sources.keys() if col not in ['文件名', '工作表名']]
    logger.info(f"📋 发现 {len(data_columns)} 个不同的数据列及其来源:")

    for i, col in enumerate(sorted(data_columns), 1):
        sources = column_sources[col]
        if len(sources) == 1:
            source_info = list(sources)[0]
            logger.info(f"   {i:2d}. {col}")
            logger.info(f"       └─ 来源: {source_info[0]} -> {source_info[1]}")
        else:
            logger.info(f"   {i:2d}. {col}")
            logger.info(f"       └─ 来源于 {len(sources)} 个工作表:")
            for source in sorted(sources):
                logger.info(f"          • {source[0]} -> {source[1]}")


def calculate_completeness_stats(df: pd.DataFrame) -> List[Dict]:
    """计算数据完整性统计"""
    data_cols = [col for col in df.columns if col not in ['文件名', '工作表名']]
    completeness_stats = []

    for col in data_cols:
        non_null_count = df[col].notna().sum()
        completeness_rate = (non_null_count / len(df)) * 100
        completeness_stats.append({
            'column': col,
            'non_null_count': non_null_count,
            'completeness_rate': completeness_rate
        })

    return sorted(completeness_stats, key=lambda x: x['completeness_rate'], reverse=True)


def log_completeness_analysis(completeness_stats: List[Dict]) -> None:
    """记录数据完整性分析"""
    logger.info(f"\n🔍 数据完整性分析:")
    logger.info("   列名\t\t\t\t非空值数量\t完整率")
    logger.info("   " + "-" * 60)

    for stat in completeness_stats:
        col_name = stat['column']
        # 根据列名长度调整tab数量
        if len(col_name) < 8:
            tabs = "\t\t\t\t"
        elif len(col_name) < 16:
            tabs = "\t\t\t"
        elif len(col_name) < 24:
            tabs = "\t\t"
        else:
            tabs = "\t"

        logger.info(f"   {col_name}{tabs}{stat['non_null_count']:,}\t\t{stat['completeness_rate']:.1f}%")


def log_processing_summary(processing_stats: Dict[str, Any], final_df: pd.DataFrame) -> None:
    """记录处理总结信息"""
    logger.info(f"\n" + "=" * 80)
    logger.info("📊 处理完成 - 详细统计报告")
    logger.info("=" * 80)

    logger.info("📁 文件处理统计:")
    logger.info(f"   • 总文件数: {processing_stats['total_files']}")
    logger.info(f"   • 成功处理: {processing_stats['processed_files']} 个文件")
    logger.info(f"   • 跳过文件: {processing_stats['skipped_files']} 个文件")
    if processing_stats.get('error_files'):
        logger.info(f"   • 错误文件: {len(processing_stats['error_files'])} 个文件")

    logger.info(f"\n📊 工作表处理统计:")
    logger.info(f"   • 总工作表数: {processing_stats['total_sheets']}")
    logger.info(f"   • 成功处理: {processing_stats['processed_sheets']} 个工作表")
    logger.info(f"   • 跳过工作表: {processing_stats['skipped_sheets']} 个工作表")

    logger.info(f"\n📈 数据统计:")
    logger.info(f"   • 总行数: {len(final_df):,}")
    logger.info(f"   • 总列数: {len(final_df.columns)}")
    logger.info(f"   • 数据列数: {len(final_df.columns) - 2}")
    logger.info(f"   • 来源信息列: 2 (文件名, 工作表名)")

    # 按文件分组统计
    logger.info(f"\n📋 按文件详细统计:")
    for detail in processing_stats.get('files_details', []):
        if detail.get('sheets_processed', 0) > 0:
            logger.info(f"   📂 {detail['relative_path']}")
            logger.info(f"      └─ 处理工作表: {detail['sheets_processed']}/{detail['total_sheets']}")
            logger.info(f"      └─ 数据行数: {detail['total_data_rows']:,}")

    # 按来源统计数据分布
    logger.info(f"\n🗂️  数据来源分布:")
    source_stats = final_df.groupby(['文件名', '工作表名']).size().reset_index()
    source_stats.columns = ['文件名', '工作表名', '行数']
    for _, row in source_stats.iterrows():
        logger.info(f"   • {row['文件名']} -> {row['工作表名']}: {row['行数']:,} 行")

    # 错误文件详情
    if processing_stats.get('error_files'):
        logger.info(f"\n❌ 错误文件详情:")
        for error in processing_stats['error_files']:
            logger.warning(f"   • {error['relative_path']}: {error['error']}")


def log_comprehensive_summary(scan_summary: Dict[str, Any], processing_stats: Dict[str, Any], final_df: pd.DataFrame) -> None:
    """记录全面的处理总结，确保无遗漏"""
    logger.info(f"\n" + "=" * 80)
    logger.info("🎯 全面扫描与处理总结 - 确保零遗漏")
    logger.info("=" * 80)

    # 扫描覆盖率
    logger.info("📁 目录扫描覆盖率:")
    logger.info(f"   • 扫描根目录: 1 个")
    logger.info(f"   • 发现子文件夹: {scan_summary['total_subdirs']} 个")
    logger.info(f"   • 包含Excel的子文件夹: {scan_summary['subdirs_with_excel']} 个")

    if scan_summary['total_subdirs'] > 0:
        coverage_rate = (scan_summary['subdirs_with_excel'] / scan_summary['total_subdirs']) * 100
        logger.info(f"   • 子文件夹覆盖率: {coverage_rate:.1f}%")

    # 文件扫描详情
    logger.info(f"\n📊 文件扫描详情:")
    logger.info(f"   • 全部Excel文件扫描: {scan_summary['all_excel_files']} 个")
    logger.info(f"   • 符合条件的目标文件: {scan_summary['target_excel_files']} 个")
    logger.info(f"   • 不符合条件的文件: {scan_summary['all_excel_files'] - scan_summary['target_excel_files']} 个")

    if scan_summary['all_excel_files'] > 0:
        match_rate = (scan_summary['target_excel_files'] / scan_summary['all_excel_files']) * 100
        logger.info(f"   • 文件匹配率: {match_rate:.1f}%")

    # 处理覆盖率分析
    logger.info(f"\n🎯 处理覆盖率分析:")
    logger.info(f"   • 目标文件数: {processing_stats['total_files']}")
    logger.info(f"   • 成功处理文件: {processing_stats['processed_files']}")
    logger.info(f"   • 跳过/失败文件: {processing_stats['skipped_files']}")

    if processing_stats['total_files'] > 0:
        success_rate = (processing_stats['processed_files'] / processing_stats['total_files']) * 100
        logger.info(f"   • 文件处理成功率: {success_rate:.1f}%")

    # 工作表级别覆盖率
    logger.info(f"\n📋 工作表级别覆盖率:")
    logger.info(f"   • 发现工作表总数: {processing_stats['total_sheets']}")
    logger.info(f"   • 包含目标数据的工作表: {processing_stats['processed_sheets']}")
    logger.info(f"   • 不包含目标数据的工作表: {processing_stats['skipped_sheets']}")

    if processing_stats['total_sheets'] > 0:
        sheet_success_rate = (processing_stats['processed_sheets'] / processing_stats['total_sheets']) * 100
        logger.info(f"   • 工作表数据提取成功率: {sheet_success_rate:.1f}%")

    # 数据完整性保证
    logger.info(f"\n✅ 数据完整性保证:")
    logger.info(f"   • 最终数据行数: {len(final_df):,}")
    logger.info(f"   • 数据来源追踪: 完整保留文件名和工作表名")
    logger.info(f"   • 列合并策略: 所有列做并集，缺失值用NaN填充")

    # 潜在遗漏提醒
    if scan_summary['all_excel_files'] > scan_summary['target_excel_files']:
        missed_count = scan_summary['all_excel_files'] - scan_summary['target_excel_files']
        logger.info(f"\n⚠️  潜在遗漏提醒:")
        logger.info(f"   • 发现 {missed_count} 个Excel文件未处理（不符合'优'开头条件）")
        logger.info(f"   • 如需处理这些文件，请检查文件命名或调整搜索条件")

    if processing_stats['skipped_files'] > 0:
        logger.info(f"\n⚠️  处理异常文件:")
        logger.info(f"   • {processing_stats['skipped_files']} 个文件处理失败或跳过")
        logger.info(f"   • 请检查错误详情确保无重要数据遗漏")

    if processing_stats['skipped_sheets'] > 0:
        logger.info(f"\n⚠️  未提取数据的工作表:")
        logger.info(f"   • {processing_stats['skipped_sheets']} 个工作表未找到目标字符串'身份证号'")
        logger.info(f"   • 如这些工作表包含重要数据，请检查表头设置")

    # 最终确认
    logger.info(f"\n🔒 处理完整性确认:")
    total_possible_data = scan_summary['target_excel_files']
    actually_processed = processing_stats['processed_files']

    if actually_processed == total_possible_data:
        logger.info(f"   ✅ 完美处理: 所有符合条件的文件均已成功处理")
    else:
        logger.info(f"   ⚠️  处理率: {actually_processed}/{total_possible_data} 个文件被处理")
        logger.info(f"   💡 建议检查失败文件确保无重要数据遗漏")


def create_summary(result: ProcessResult) -> Dict[str, Any]:
    """创建处理摘要"""
    df = result.data
    return {
        'files_processed': result.files_processed,
        'sheets_processed': result.sheets_processed,
        'total_rows': result.total_rows,
        'total_columns': len(df.columns),
        'data_columns': len(df.columns) - 2,
        'completeness': {
            col: f"{df[col].notna().sum() / len(df) * 100:.1f}%"
            for col in df.columns if col not in ['文件名', '工作表名']
        },
        'sources': df.groupby(['文件名', '工作表名']).size().to_dict()
    }


def process_excel_files(folder_path: str) -> ProcessResult:
    """
    主处理函数 - 大师级实现with详细日志

    Args:
        folder_path: Excel文件夹路径

    Returns:
        ProcessResult: 包含处理结果和统计信息
    """
    logger.info("=" * 80)
    logger.info("🔍 开始处理Excel文件...")
    logger.info("=" * 80)

    path = Path(folder_path)
    excel_files, scan_summary = get_excel_files(path)

    logger.info("\n" + "=" * 80)
    logger.info("🔄 开始处理文件...")
    logger.info("=" * 80)

    # 初始化统计信息
    processing_stats = {
        'total_files': len(excel_files),
        'processed_files': 0,
        'skipped_files': 0,
        'total_sheets': 0,
        'processed_sheets': 0,
        'skipped_sheets': 0,
        'files_details': [],
        'error_files': []
    }

    # 处理所有文件
    all_dataframes = []
    for file_path in excel_files:
        file_dfs, file_stats = process_file(file_path)

        if 'error' in file_stats:
            processing_stats['error_files'].append(file_stats['error'])
            processing_stats['skipped_files'] += 1
            print(file_path)
        else:
            all_dataframes.extend(file_dfs)
            processing_stats['total_sheets'] += file_stats.get('total_sheets', 0)
            processing_stats['processed_sheets'] += file_stats.get('sheets_processed', 0)
            processing_stats['skipped_sheets'] += file_stats.get('sheets_skipped', 0)

            if file_stats.get('sheets_processed', 0) > 0:
                processing_stats['processed_files'] += 1
            else:
                processing_stats['skipped_files'] += 1
                print(file_path)

            processing_stats['files_details'].append(file_stats)

    if not all_dataframes:
        logger.error("\n❌ 没有成功处理任何数据")
        raise ValueError("没有成功处理任何数据")

    # 合并数据
    logger.info(f"\n" + "=" * 80)
    logger.info("🔗 开始合并数据...")
    logger.info("=" * 80)

    # 收集列来源信息
    column_sources = collect_column_sources(all_dataframes)
    log_column_sources(column_sources)

    # 统一列结构并合并
    unified_dfs = unify_columns(all_dataframes)
    final_df = pd.concat(unified_dfs, ignore_index=True)

    # 计算完整性统计
    completeness_stats = calculate_completeness_stats(final_df)

    # 创建结果
    result = ProcessResult(
        data=final_df,
        files_processed=processing_stats['processed_files'],
        sheets_processed=processing_stats['processed_sheets'],
        total_rows=len(final_df),
        column_sources=column_sources,
        processing_stats=processing_stats,
        scan_summary=scan_summary
    )

    # 记录详细统计信息
    log_processing_summary(processing_stats, final_df)
    log_completeness_analysis(completeness_stats)

    # 记录全面总结，确保零遗漏
    log_comprehensive_summary(scan_summary, processing_stats, final_df)

    logger.info(f"\n" + "=" * 80)
    logger.info("🎉 处理完成！数据已准备就绪。")
    logger.info("=" * 80)

    return result


def save_excel(df: pd.DataFrame, output_path: str) -> None:
    """保存为Excel文件"""
    try:
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"💾 已保存到: {output_path}")
    except Exception as e:
        logger.error(f"❌ 保存失败: {e}")


def main() -> Optional[pd.DataFrame]:
    """主函数 - 命令行接口"""
    try:
        folder_path = input("📁 Excel文件夹路径: ").strip()

        # 处理数据
        result = process_excel_files(folder_path)

        # 显示预览
        print("\n" + "="*50)
        print("📊 数据预览 (前5行):")
        print(result.data.head().to_string())

        # 保存选项
        if input("\n💾 保存结果? (y/N): ").lower() == 'y':
            output_path = input("📄 输出文件名: ").strip()
            if not output_path.endswith('.xlsx'):
                output_path += '.xlsx'
            save_excel(result.data, output_path)

        return result.data

    except (KeyboardInterrupt, EOFError):
        logger.info("👋 用户取消操作")
        return None
    except Exception as e:
        logger.error(f"❌ 处理失败: {e}")
        return None


if __name__ == "__main__":
    main()
