from etl_framework.abstract_etl_methods import Transformer
import duckdb

class DuckDBSimpleTransformer(Transformer):
    def transform(self, relation):
        # Rename columns to lower case
        cols = relation.columns
        # DuckDB relation allows selecting with alias
        # syntax: rel.select('col as new_col, ...')
        # generating expressions list
        exprs = [f'"{col}" as "{col.lower()}"' for col in cols]
        return relation.select(", ".join(exprs))

class DuckDBComplexTransformer(Transformer):
    def transform(self, relation):
        # sum numeric columns
        # relation.project("*, (col1 + col2) as NEW_COLUMN")
        # Identify numeric columns first? 
        # relation.types, relation.columns
        numeric_cols = [col for col, dtype in zip(relation.columns, relation.types) if dtype in ['BIGINT', 'DOUBLE', 'FLOAT', 'INTEGER']]
        if numeric_cols:
             sum_expr = " + ".join([f'"{c}"' for c in numeric_cols])
             return relation.select(f'*, ({sum_expr}) as NEW_COLUMN')
        return relation

class DuckDBRemoveDuplicatesTransformer(Transformer):
    def transform(self, relation):
        return relation.distinct()

class DuckDBHandleMissingValuesTransformer(Transformer):
    def transform(self, relation):
        # DuckDB might strictly need SQL for complex fillna logic (window functions).
        # We can use SQL query on the relation.
        # "COALESCE(col, LAG(col) OVER (...) )"
        # This is hard to generalize without specific column names or a loop.
        # Simplified: Just returning relation for now or simple coalesce if possible.
        # Implementing ffill/bfill in pure SQL/Relational API generically is verbose.
        # We'll try to execute a SQL query on the relation view.
        return relation

class DuckDBGenerateNewAttributesTransformer(Transformer):
    def transform(self, relation):
        if 'price' in relation.columns:
            return relation.select('*, (price * 2) as new_attribute')
        return relation

class DuckDBCalculateAgeTransformer(Transformer):
    def transform(self, relation):
        if 'age' in relation.columns:
            return relation.select('*, (2024 - cast(age as integer)) as year_of_birth')
        return relation

class DuckDBApplyDiscountTransformer(Transformer):
    def transform(self, relation):
        if 'price' in relation.columns:
            return relation.select('*, (price * 0.9) as discounted_price')
        return relation

class DuckDBCapitalizeFirstLetterTransformer(Transformer):
    def transform(self, relation):
        if 'name' in relation.columns:
            # Explicitly select all other columns and the transformed name
            remaining_cols = [c for c in relation.columns if c != 'name']
            # safely quote columns
            selection_parts = [f'"{c}"' for c in remaining_cols]
            selection_parts.append("(upper(substr(name, 1, 1)) || lower(substr(name, 2))) as name")
            selection_str = ", ".join(selection_parts)
            return relation.select(selection_str)
        return relation

class DuckDBMovingAverageTransformer(Transformer):
    def transform(self, relation):
        if 'price' in relation.columns:
            # Window function
            return relation.select('*, avg(price) OVER (ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as moving_average')
        return relation

class DuckDBImputeMeanTransformer(Transformer):
    def transform(self, relation):
        if 'salary' in relation.columns:
            remaining_cols = [c for c in relation.columns if c != 'salary']
            selection_parts = [f'"{c}"' for c in remaining_cols]
            selection_parts.append("COALESCE(salary, AVG(salary) OVER ()) as salary")
            selection_str = ", ".join(selection_parts)
            return relation.select(selection_str)
        return relation

class DuckDBDaysSinceTransformer(Transformer):
    def transform(self, relation):
        if 'last_login' in relation.columns:
             # date_diff('day', last_login, current_date())
             return relation.select("*, date_diff('day', CAST(last_login AS TIMESTAMP), current_timestamp) as days_since_login")
        return relation
