from src.controller.answer import _format_reasoning_result
import json

res = {'query_type': 'single_agency_statistics', 'count': 500, 'total_records': 1953, 'breakdown': {'top_offenders': [{'insight_id': 'INS-00000867', 'offense_count': 20}, {'insight_id': 'INS-00000158', 'offense_count': 20}, {'insight_id': 'INS-00000886', 'offense_count': 19}, {'insight_id': 'INS-00000806', 'offense_count': 19}, {'insight_id': 'INS-00000783', 'offense_count': 19}, {'insight_id': 'INS-00000763', 'offense_count': 18}, {'insight_id': 'INS-00000883', 'offense_count': 18}, {'insight_id': 'INS-00000694', 'offense_count': 17}, {'insight_id': 'INS-00000019', 'offense_count': 17}, {'insight_id': 'INS-00000733', 'offense_count': 17}]}, 'agency': 'idjc', 'confidence': 0.95, 'graph_stats': {'nodes': 1, 'edges': 0}, 'timestamp': '2026-04-09T16:07:56.141286+00:00'}

print(_format_reasoning_result(res))
