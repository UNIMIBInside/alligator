import asyncio
import traceback
import json
from model.row import Row

 
class Lookup:
    def __init__(self, data:object, lamAPI, target, log_c, kg_ref="wikidata", limit=100):
        self._header = data.get("header", [])
        self._dataset_name = data["datasetName"]
        self._table_name = data["tableName"]
        self._types = data.get("Ctypes", {})
        self._NERTypes = data.get("NERTypes", {})
        self._t_closure = data.get("t_closure",{})
        self._lamAPI = lamAPI
        self._target = target
        self._log_c = log_c
        self._kg_ref = kg_ref
        self._limit = limit
        self._rows_data = data["rows"]
        self._rows = []
        self._cache = {}
       
    async def generate_candidates(self):
        tasks = []
        for row in self._rows_data:
            tasks.append(asyncio.create_task(self._build_row(row["data"], row["idRow"],row.get("ids",None))))
        results = await asyncio.gather(*tasks)
        for row in results:
            self._rows.append(row)

    async def _build_row(self, cells, id_row,ids=None):
        row = Row(id_row, len(cells))
        cells_as_strings = [str(cell) for cell in cells]
        row_text = " ".join(cells_as_strings)
        num_type = 0
        for i, cell in enumerate(cells):
            if i in self._target["NE"]:
                qid = ids[i] if ids is not None else None
                if self._types is not None and num_type < len(self._types):
                    types = self._types[num_type]
                    if self._t_closure is not None and num_type < len(self._t_closure):
                        t_closure = self._t_closure[num_type]
                    else:
                        t_closure = None
                    num_type += 1
                else:
                    types = None
                    t_closure = None
                description = " ".join(list(set(cells_as_strings) - set([cell]))) # unused
                if self._NERTypes is not None and num_type < len(self._NERTypes):
                    NERType = self._NERTypes[num_type]
                    num_type += 1
                else: 
                    NERType = None
                if cell in self._cache:
                    candidates = self._cache.get(cell, [])
                else:
                    candidates = await self._get_candidates(cell, id_row, types, NERType,qid, t_closure)
                    self._cache[cell] = candidates
                is_subject = i == self._target["SUBJ"]
                row.add_ne_cell(cell, row_text, candidates, i, is_subject,qid=qid)
            elif i in self._target["LIT"]:
                row.add_lit_cell(cell, i, self._target["LIT_DATATYPE"][str(i)])
            else:    
                row.add_notag_cell(cell, i)
        return row

    
    async def _get_candidates(self, cell, id_row, types, NERType, qid=None, t_closure = None):
        candidates = []
        result = None
        try:
            if len(str(cell)) > 0 and str(cell).lower() != "nan":
                result = await self._lamAPI.lookup(cell, types=types, NERTypes=NERType, limit=100, ids=qid, t_closure = t_closure)
                if 'error' in result or len(result) == 0:
                    raise Exception("Error from lamAPI")
                candidates = result["wikidata"]
        except Exception as e:
            self._log_c.insert_one({
                'datasetName': self._dataset_name,
                'tableName': self._table_name,
                'idRow': id_row,
                'cell': cell,
                'types': types,
                'error': str(e), 
                'stackTrace': traceback.format_exc(),
                'result': result
            })
            return []
            
        return candidates

    
    def get_rows(self):
        return self._rows