 
 /* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/27.
//

#include "common/log/log.h"
#include "sql/operator/aggregate_physical_operator.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"


//#include "sql/stmt/delete_stmt.h"

Tuple *AggregatePhysicalOperator::current_tuple()
{
  LOG_TRACE("return current tuple");
  return &result_tuple_;
}

void AggregatePhysicalOperator::add_aggregation(const AggrOp aggregation) {
  aggregations_.push_back(aggregation);
}


RC AggregatePhysicalOperator::open(Trx *trx)
{
  if (children_.size()!=1) {
    LOG_WARN("aggregate operator should contain only one child");
    return RC::INTERNAL;
  }
  return children_[0]->open(trx);
}


RC AggregatePhysicalOperator::next()
{
  // already aggregated
  if (result_tuple_.cell_num() > 0) {
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  PhysicalOperator *oper = children_[0].get();

  int cnt = 0;

  std::vector<Value> result_cells;
  while (RC::SUCCESS == (rc = oper->next())) {

    Tuple *tuple = oper->current_tuple();

    for (int cell_idx = 0; cell_idx < (int)aggregations_.size(); cell_idx++) {
      const AggrOp aggregation = aggregations_[cell_idx];

      Value cell;
      AttrType attr_type = AttrType::INTS;

      switch (aggregation) {

        case AggrOp::AGGR_SUM:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (!cnt){
            result_cells.push_back(Value(0.0f));
          }
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;

        case AggrOp::AGGR_AVG:
          rc = tuple->cell_at(cell_idx, cell);
          attr_type = cell.attr_type();
          if (!cnt){
            result_cells.push_back(Value(0.0f));
          }
          if (attr_type == AttrType::INTS or attr_type == AttrType::FLOATS) {
            result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() + cell.get_float());
          }
          break;

        case AggrOp::AGGR_MAX:
          rc = tuple->cell_at(cell_idx, cell);
          if (!cnt){
            result_cells.push_back(cell);
          }else if(cell.compare(result_cells[cell_idx]) > 0){
            result_cells[cell_idx] = cell;
          }
          break;
        
        case AggrOp::AGGR_MIN:
          rc = tuple->cell_at(cell_idx, cell);
          if (!cnt){
            result_cells.push_back(cell);
          }else if(cell.compare(result_cells[cell_idx]) < 0){
            result_cells[cell_idx] = cell;
          }
          break;

        case AggrOp::AGGR_COUNT:
          if (cnt == 0) {
            result_cells.push_back(Value(0));
          }
          result_cells[cell_idx].set_int(result_cells[cell_idx].get_int() + 1);
          break;

        default:
          return RC::UNIMPLENMENT;
      }
    }

    cnt++;
    
  }
  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  for (int cell_idx = 0; cell_idx < (int)result_cells.size(); cell_idx++) {
    const AggrOp aggr = aggregations_[cell_idx];
    if (aggr == AggrOp::AGGR_AVG) {
      result_cells[cell_idx].set_float(result_cells[cell_idx].get_float() / cnt);
    }
  }
  
  result_tuple_.set_cells(result_cells);

  return rc;
}




RC AggregatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

 
 
 
 
 
 
 
 
 
 
 
 
 
