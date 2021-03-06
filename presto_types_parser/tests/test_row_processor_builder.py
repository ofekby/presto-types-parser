from unittest import TestCase
from mock import patch, MagicMock
from presto_types_parser.src.row_parser_builder import build_row_parser
from presto_types_parser.src.row_parser import PrestoRowParser
from presto_types_parser.src.column_process.default_cell_processor import process_raw_cell \
    as process_default_cell
from presto_types_parser.src.column_process.varbinary_processor import process_raw_cell \
    as process_varbinary


class TestPrestoRowProcessorBuilder(TestCase):
    def test_given_columns_when_build_row_parser_should_return_expected_row_parser(self):
        mocked_process_array = MagicMock()
        mocked_process_inner_row = MagicMock()
        mocked_process_map = MagicMock()

        array_patch = patch(
            "presto_types_parser.src.complex_column_process.array_processor_builder"
            ".new_process_raw_cell_function", new=MagicMock(return_value=mocked_process_array))
        inner_row_patch = patch(
            "presto_types_parser.src.complex_column_process.inner_row_processor_builder."
            "new_inner_row_process_function",
            new=MagicMock(return_value=mocked_process_inner_row))
        map_patch = patch("presto_types_parser.src.complex_column_process.map_processor_builder."
                          "new_map_process_function",
                          new=MagicMock(return_value=mocked_process_map))

        mocked_new_process_array = array_patch.start()
        mocked_new_process_inner_row = inner_row_patch.start()
        mocked_new_process_map = map_patch.start()

        columns = [
            {
                "name": "some_field",
                "type": "row(v1 integer, "
                        "v2 row(v3 integer, v4 integer), "
                        "array(row(integer)), "
                        "b1 varbinary)",
                "typeSignature": {
                    "rawType": "row",
                    "arguments": [
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "v1"
                                },
                                "typeSignature": {
                                    "rawType": "integer",
                                    "arguments": []
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "v2"
                                },
                                "typeSignature": {
                                    "rawType": "row",
                                    "arguments": [
                                        {
                                            "kind": "NAMED_TYPE",
                                            "value": {
                                                "fieldName": {
                                                    "name": "v3"
                                                },
                                                "typeSignature": {
                                                    "rawType": "integer",
                                                    "arguments": []
                                                }
                                            }
                                        },
                                        {
                                            "kind": "NAMED_TYPE",
                                            "value": {
                                                "fieldName": {
                                                    "name": "v4"
                                                },
                                                "typeSignature": {
                                                    "rawType": "integer",
                                                    "arguments": []
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "typeSignature": {
                                    "rawType": "array",
                                    "arguments": [
                                        {
                                            "kind": "TYPE",
                                            "value": {
                                                "rawType": "row",
                                                "arguments": [
                                                    {
                                                        "kind": "NAMED_TYPE",
                                                        "value": {
                                                            "typeSignature": {
                                                                "rawType": "integer",
                                                                "arguments": []
                                                            }
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "kind": "NAMED_TYPE",
                            "value": {
                                "fieldName": {
                                    "name": "b1"
                                },
                                "typeSignature": {
                                    "rawType": "varbinary",
                                    "arguments": []
                                }
                            }
                        }
                    ]
                }
            },
            {
                "name": "some_binary",
                "type": "varbinary",
                "typeSignature": {
                    "rawType": "varbinary",
                    "arguments": []
                }
            },
            {
                "name": "some_map",
                "type": "map(varchar(1), integer)",
                "typeSignature": {
                    "rawType": "map",
                    "arguments": [
                        {
                            "kind": "TYPE",
                            "value": {
                                "rawType": "varchar",
                                "arguments": [
                                    {
                                        "kind": "LONG",
                                        "value": 1
                                    }
                                ]
                            }
                        },
                        {
                            "kind": "TYPE",
                            "value": {
                                "rawType": "integer",
                                "arguments": []
                            }
                        }
                    ]
                }
            }
        ]

        expected_row_parser = PrestoRowParser(
            root_cell_processors=[
                mocked_process_inner_row,
                process_varbinary,
                mocked_process_map
            ]
        )

        built_row_parser = build_row_parser(columns)

        mocked_new_process_inner_row.assert_any_call(
            ["v1", "v2", "field2", "b1"],
            [
                process_default_cell,
                mocked_process_inner_row,
                mocked_process_array,
                process_varbinary
            ]
        )
        mocked_new_process_inner_row.assert_any_call(
            ["v3", "v4"],
            [process_default_cell, process_default_cell]
        )
        mocked_new_process_array.assert_any_call(
            mocked_process_inner_row
        )
        mocked_new_process_inner_row.assert_any_call(
            ["field0"],
            [process_default_cell]
        )
        mocked_new_process_map.assert_any_call(
            process_default_cell,
            'varchar'
        )

        self.assertEqual(
            expected_row_parser,
            built_row_parser
        )

        array_patch.stop()
        inner_row_patch.stop()
        map_patch.stop()
