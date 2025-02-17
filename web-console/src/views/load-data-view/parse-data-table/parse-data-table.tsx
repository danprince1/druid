/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import React from 'react';
import type { RowRenderProps } from 'react-table';
import ReactTable from 'react-table';

import { TableCell, TableCellUnparseable } from '../../../components';
import type { FlattenField } from '../../../druid-models';
import {
  DEFAULT_TABLE_CLASS_NAME,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../../react-table';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import type { SampleEntry, SampleResponse } from '../../../utils/sampler';
import { getHeaderNamesFromSampleResponse } from '../../../utils/sampler';

import './parse-data-table.scss';

export interface ParseDataTableProps {
  sampleResponse: SampleResponse;
  columnFilter: string;
  canFlatten: boolean;
  flattenedColumnsOnly: boolean;
  flattenFields: FlattenField[];
  onFlattenFieldSelect: (field: FlattenField, index: number) => void;
  useInput?: boolean;
}

export const ParseDataTable = React.memo(function ParseDataTable(props: ParseDataTableProps) {
  const {
    sampleResponse,
    columnFilter,
    canFlatten,
    flattenedColumnsOnly,
    flattenFields,
    onFlattenFieldSelect,
    useInput,
  } = props;

  const key = useInput ? 'input' : 'parsed';
  return (
    <ReactTable
      className={classNames('parse-data-table', DEFAULT_TABLE_CLASS_NAME)}
      data={sampleResponse.data}
      sortable={false}
      defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
      pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
      showPagination={sampleResponse.data.length > STANDARD_TABLE_PAGE_SIZE}
      columns={filterMap(
        getHeaderNamesFromSampleResponse(sampleResponse, true),
        (columnName, i) => {
          if (!caseInsensitiveContains(columnName, columnFilter)) return;
          const flattenFieldIndex = flattenFields.findIndex(f => f.name === columnName);
          if (flattenFieldIndex === -1 && flattenedColumnsOnly) return;
          const flattenField = flattenFields[flattenFieldIndex];
          return {
            Header: (
              <div
                className={classNames({ clickable: flattenField })}
                onClick={() => {
                  if (!flattenField) return;
                  onFlattenFieldSelect(flattenField, flattenFieldIndex);
                }}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {flattenField ? `${flattenField.type}: ${flattenField.expr}` : ''}&nbsp;
                </div>
              </div>
            ),
            id: String(i),
            accessor: (row: SampleEntry) => (row[key] ? row[key]![columnName] : null),
            width: 140,
            Cell: function ParseDataTableCell(row: RowRenderProps) {
              if (row.original.unparseable) {
                return <TableCellUnparseable />;
              }
              return <TableCell value={row.value} />;
            },
            headerClassName: classNames({
              flattened: flattenField,
            }),
          };
        },
      )}
      SubComponent={rowInfo => {
        const { input, error } = rowInfo.original;
        const inputStr = JSONBig.stringify(input, undefined, 2);

        if (!error && input && canFlatten) {
          return <pre className="parse-detail">{'Original row: ' + inputStr}</pre>;
        } else {
          return (
            <div className="parse-detail">
              {error && <div className="parse-error">{error}</div>}
              <div>{'Original row: ' + inputStr}</div>
            </div>
          );
        }
      }}
    />
  );
});
