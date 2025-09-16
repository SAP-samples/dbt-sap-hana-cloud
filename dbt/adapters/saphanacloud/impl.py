from dataclasses import dataclass
from datetime import datetime, timezone
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.saphanacloud import SapHanaCloudConnectionManager
from dbt.adapters.saphanacloud.relation import SapHanaCloudRelation
from dbt.adapters.base import AdapterConfig
from dbt.adapters.base.relation import BaseRelation
from dbt_common.contracts.constraints import ConstraintType, ColumnLevelConstraint, ModelLevelConstraint
from dbt_common.dataclass_schema import ValidationError, dbtClassMixin
from dbt.adapters.base import available
from typing import Any, List, Dict, Optional, FrozenSet, Tuple, Set, Iterable
import agate
from dbt_common.exceptions import MacroArgTypeError, CompilationError
from dbt.adapters.base.impl import GET_CATALOG_RELATIONS_MACRO_NAME, ConstraintSupport
from dbt.context.providers import generate_runtime_model_context
from dbt.adapters.saphanacloud.column import SapHanaCloudColumn
from dbt.adapters.exceptions import IndexConfigError, IndexConfigNotDictError
from dbt_common.utils import encoding as dbt_encoding
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
import logging


@dataclass
class SapHanaCloudIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.now(timezone.utc).isoformat()
        inputs = self.columns + \
            [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return dbt_encoding.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["SapHanaCloudIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass
class SapHanaCloudConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[SapHanaCloudIndexConfig]] = None


class SapHanaCloudAdapter(SQLAdapter):
    logging.basicConfig(level=logging.INFO)
    ConnectionManager = SapHanaCloudConnectionManager
    Relation = SapHanaCloudRelation
    column = SapHanaCloudColumn

    AdapterSpecificConfigs = SapHanaCloudConfig

    CATALOG_BY_RELATION_SUPPORT = True

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }

    _capabilities = CapabilityDict(
        {Capability.SchemaMetadataByRelations: CapabilitySupport(
            support=Support.Full)}
    )

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False):

        # Ensure BEGIN is not used explicitly, as it is not required in HANA
        if "BEGIN" in sql.upper():
            print(
                "BEGIN command found, skipping execution as HANA does not require this.")
            return None

        # Execute the query
        rel1 = super().add_query(sql, False, bindings, abridge_sql_log)

        return rel1

    def debug_query(self) -> None:
        self.execute("SELECT 1 FROM DUMMY")

    @classmethod
    def date_function(cls):
        return 'CURRENT_DATE'

    def convert_boolean_type(self, value):
        return 'BOOLEAN'

    def convert_date_type(self, value):
        return 'DATE'

    def convert_datetime_type(self, value):
        return 'TIMESTAMP'

    @classmethod
    def convert_number_type(cls, agate_table, col_idx):
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "FLOAT" if decimals else "INTEGER"

    def convert_text_type(self, value):
        return 'VARCHAR'

    def convert_time_type(self, value):
        return 'TIME'

    def alter_column_type(self, relation, column_name, new_column_type) -> None:
        self.execute_macro("saphanacloud__alter_column_type", kwargs={
                           "relation": relation, "column_name": column_name, "new_column_type": new_column_type})

    def check_schema_exists(self, schema_name: str) -> bool:
        results = self.execute_macro("saphanacloud__check_schema_exists", kwargs={
                                     "schema_name": schema_name})
        return len(results) > 0

    def create_schema(self, relation: BaseRelation):
        schema_name = relation.schema
        self.execute_macro("saphanacloud__create_schema",
                           kwargs={"schema_name": schema_name})

    def drop_relation(self, relation: BaseRelation):
        self.execute_macro("saphanacloud__drop_relation",
                           kwargs={"relation": relation})

    def drop_schema(self, relation: BaseRelation):
        relations = self.list_relations(
            database=relation.database,
            schema=relation.schema
        )

        for rel in relations:
            self.drop_relation(rel)

        super().drop_schema(relation)

    def expand_column_types(self, from_relation: BaseRelation, to_relation: BaseRelation) -> None:
        pass

    @available
    def parse_index(self, raw_index: Any) -> Optional[SapHanaCloudIndexConfig]:
        return SapHanaCloudIndexConfig.parse(raw_index)

    def get_columns_in_relation(self, relation: SapHanaCloudRelation):
        results = self.execute_macro(
            "saphanacloud__get_columns_in_relation", kwargs={"relation": relation})
        # Ensure results are not None
        if results:
            # `results` is an `agate.MappedSequence`, so we need to extract the values from the columns
            return results
        else:
            return []

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = 'hour'
    ) -> str:
        # Mapping intervals to SAP HANA functions
        if interval == 'hour':
            return f"ADD_SECONDS({add_to}, {number} * 3600)"
        elif interval == 'day':
            return f"ADD_DAYS({add_to}, {number})"
        elif interval == 'year':
            return f"ADD_YEARS({add_to}, {number})"
        elif interval == 'minute':
            return f"ADD_SECONDS({add_to}, {number} * 60)"
        elif interval == 'second':
            return f"ADD_SECONDS({add_to}, {number})"
        else:
            raise ValueError(f"Unsupported interval: {interval}")

    def is_cancelable(self):
        return True

    def get_catalog_by_relations(self, used_schemas: FrozenSet[Tuple[str, str]], relations: List[BaseRelation]):
        # Prepare arguments for the macro call
        # Or the equivalent schema in SAP HANA
        information_schema = 'INFORMATION_SCHEMA'
        # Call the macro to get catalog data for specific relations
        catalog_table = self.execute_macro(
            # Using the constant for the macro name
            macro_name=GET_CATALOG_RELATIONS_MACRO_NAME,
            kwargs={
                "information_schema": information_schema,
                "relations": relations
            }
        )

        return catalog_table, []

    def get_filtered_catalog(
        self,
        relation_configs: Iterable[RelationConfig],
        used_schemas: FrozenSet[Tuple[str, str]],
        relations: Optional[Set[BaseRelation]] = None
    ):
        catalogs = None
        exceptions = []

        # Use full catalog if relations are not provided or if we cannot fetch by relations
        if (
            relations is None
            or len(relations) == 0
            or len(relations) > 100
            or not self.supports(Capability.SchemaMetadataByRelations)
        ):

            catalogs, exceptions = self.get_catalog(
                relation_configs, used_schemas)
        else:

            catalogs, exceptions = self.get_catalog_by_relations(
                used_schemas, relations)

        # Filter the catalog by relations
        if relations and catalogs:
            relation_map = {
                (r.schema.casefold(), r.identifier.casefold())
                for r in relations
            }

            def in_map(row):
                schema = row["table_schema"].lower()
                table = row["table_name"].lower()
                return (schema, table) in relation_map

            catalogs = catalogs.where(in_map)

        return catalogs, exceptions

    def list_relations_without_caching(self, relation: SapHanaCloudRelation):
        # Pass both the database and schema names to the macro
        results = self.execute_macro(
            "saphanacloud__list_relations_without_caching",
            kwargs={"database_name": relation.database,
                    "schema_name": relation.schema}
        )
        return results

    def list_schemas(self, database: str) -> List[str]:
        schemas = self.execute_macro("saphanacloud__list_schemas")
        rel2 = [schema.lower() for schema in schemas]
        return rel2

    def populate_adapter_cache(self, adapter, required_schemas):
        for schema in required_schemas:
            relations = self.list_relations_without_caching(
                BaseRelation.create(
                    database=schema.database,
                    schema=schema.schema,
                    identifier=None,
                )
            )

            for relation in relations:
                adapter.cache.add(relation)

    def get_rows_different_sql(
        self,
        relation_a: BaseRelation,
        relation_b: BaseRelation,
        column_names: Optional[List[str]] = None,
        except_operator: str = "EXCEPT",
    ) -> str:
        """Generate SQL for a query that returns a single row with a two
        columns: the number of rows that are different between the two
        relations and the number of mismatched rows.
        """
        # This method only really exists for test reasons.
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))
        columns_csv = ", ".join(names)

        sql = COLUMNS_EQUAL_SQL.format(
            columns=columns_csv,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
            except_op=except_operator,
        )

        return sql

    @available
    @classmethod
    def render_raw_columns_constraints(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_column_constraints = []
        skipped_check_constraints = []

        for v in raw_columns.values():
            col_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
            rendered_column_constraint = [f"{col_name} {v['data_type']}"]
            for con in v.get("constraints", None):
                constraint = cls._parse_column_constraint(con)
                if constraint.type == "ConstraintType.check":
                    # Skip check constraints for now
                    skipped_check_constraints.append(constraint)
                    continue
                c = cls.process_parsed_constraint(
                    constraint, cls.render_column_constraint)
                if c is not None:
                    rendered_column_constraint.append(c)

            # Process skipped check constraints at the end
            for constraint in skipped_check_constraints:
                c = cls.process_parsed_constraint(
                    constraint, cls.render_column_constraint)
                if c is not None:
                    rendered_column_constraint.append(c)

            rendered_column_constraints.append(
                " ".join(rendered_column_constraint))

        return rendered_column_constraints

    def rename_relation(self, from_relation: BaseRelation, to_relation: BaseRelation):
        self.execute_macro("saphanacloud__rename_relation", kwargs={
                           "from_relation": from_relation, "to_relation": to_relation})

    def truncate_relation(self, relation: BaseRelation):
        self.execute_macro("saphanacloud__truncate_relation",
                           kwargs={"relation": relation})

    def get_current_timestamp(self):
        return self.execute_macro("saphanacloud__current_timestamp")

    @available
    def standardize_grants_dict(self, grants_table: agate.Table) -> dict:
        """
        Standardizes the grants returned from the `SHOW GRANTS ON {{model}}` call.

        :param grants_table: An agate table containing the query result of
            the SQL returned by get_show_grant_sql
        :return: A standardized dictionary matching the `grants` config
        :rtype: dict
        """
        grants_dict: Dict[str, List[str]] = {}
        for row in grants_table:
            grantee = row["GRANTEE"]
            privilege = row["PRIVILEGE"]
            if privilege in grants_dict.keys():
                grants_dict[privilege].append(grantee)
            else:
                grants_dict.update({privilege: [grantee]})
        return grants_dict

    def get_relation(self, database: str, schema: str, identifier: str):
        """
        Retrieves a relation (table or view) from the database.


        :param database: The name of the database.
        :param schema: The name of the schema.
        :param identifier: The name of the table/view.
        :return: A Relation object if found, else None.
        """
        try:
            # Create a relation instance (dbt uses Relation objects to manage database entities)
            relation = self.Relation.create(
                database=database, schema=schema, identifier=identifier)

            # Use adapter logic to fetch relation details
            results = self.list_relations_without_caching(relation)

            # Search for the relation in the results
            for result in results:
                if result.identifier.lower() == identifier.lower():

                    return result

        except Exception as e:
            # Handle any exceptions if the table doesn't exist or another error occurs
            print(f"Error fetching relation {identifier}: {str(e)}")
        return None

    def get_timestamp_field(self, relation: SapHanaCloudRelation):
        results = self.execute_macro(
            "saphanacloud__get_timestamp_field", kwargs={"relation": relation})

        if results:
            # Expecting only one result since there's usually only one timestamp column per table
            timestamp_field = str(results[0][0]) if len(
                results[0]) > 0 else None
            return timestamp_field
        else:
            print('No timestamp field found.')
            return None

    def quote_seed_column(self, column: str, quote_config: Optional[bool]) -> str:
        quote_columns: bool = False
        if isinstance(quote_config, bool):
            quote_columns = quote_config
        elif quote_config is None:
            pass
        else:
            msg = (
                f'The seed configuration value of "quote_columns" has an '
                f"invalid type {type(quote_config)}"
            )
            raise CompilationError(msg)

        if quote_columns:
            return self.quote(column)
        else:
            return column

    def valid_incremental_strategies(self):
        return ["append", "merge", "delete+insert", "microbatch"]

    @available
    @classmethod
    def render_raw_columns_names(cls, raw_columns: Dict[str, Dict[str, Any]]) -> List:
        rendered_column_names = []

        for v in raw_columns.values():
            col_name = cls.quote(v["name"]) if v.get("quote") else v["name"]
            rendered_column_names.append(col_name)

        return rendered_column_names

    @available.parse_list
    def get_missing_columns(self, from_relation: BaseRelation, to_relation: BaseRelation) -> List[column]:
        """Returns a list of Columns in from_relation that are missing from
        to_relation.
        """

        if not isinstance(from_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="get_missing_columns",
                arg_name="from_relation",
                got_value=from_relation,
                expected_type=self.Relation,
            )

        if not isinstance(to_relation, self.Relation):
            raise MacroArgTypeError(
                method_name="get_missing_columns",
                arg_name="to_relation",
                got_value=to_relation,
                expected_type=self.Relation,
            )

        # Fetch the columns from both relations
        from_columns = self.get_columns_in_relation(from_relation)
        to_columns = self.get_columns_in_relation(to_relation)

        # Check if columns are strings or objects and handle accordingly
        if isinstance(from_columns[0], str):
            # Columns are strings, create dictionaries with column names as keys
            from_columns_dict = {col: col for col in from_columns}
            to_columns_dict = {col: col for col in to_columns}
        else:
            # Columns are objects, access `.name` attribute
            from_columns_dict = {col.name: col for col in from_columns}
            to_columns_dict = {col.name: col for col in to_columns}

        # Find the missing columns
        missing_columns = set(from_columns_dict.keys()) - \
            set(to_columns_dict.keys())

        # Return the missing columns
        return [from_columns_dict[col_name] for col_name in missing_columns]

    @classmethod
    def render_column_constraint(cls, constraint: ColumnLevelConstraint) -> Optional[str]:
        """Render the given constraint as DDL text. Should be overriden by adapters which need custom constraint
        rendering."""
        constraint_expression = constraint.expression or ""

        rendered_column_constraint = None
        if constraint.type == ConstraintType.check and constraint_expression:
            rendered_column_constraint = f",check ({constraint_expression})"
        elif constraint.type == ConstraintType.not_null:
            rendered_column_constraint = f"not null {constraint_expression}"
        elif constraint.type == ConstraintType.unique:
            rendered_column_constraint = f"unique {constraint_expression}"
        elif constraint.type == ConstraintType.primary_key:
            rendered_column_constraint = f"primary key {constraint_expression}"
        elif constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                rendered_column_constraint = (
                    f"references {constraint.to} ({', '.join(constraint.to_columns)})"
                )
            elif constraint_expression:
                rendered_column_constraint = f"references {constraint_expression}"
        elif constraint.type == ConstraintType.custom and constraint_expression:
            rendered_column_constraint = constraint_expression

        if rendered_column_constraint:
            rendered_column_constraint = rendered_column_constraint.strip()

        return rendered_column_constraint

    @available
    @classmethod
    def render_raw_model_constraints(cls, raw_constraints: List[Dict[str, Any]], raw_columns: Dict[str, Dict[str, Any]]) -> List[str]:
        return [
            c for c in map(lambda constraint: cls.render_raw_model_constraint(constraint, raw_columns), raw_constraints)
            if c is not None
        ]

    @classmethod
    def render_raw_model_constraint(cls, raw_constraint: Dict[str, Any], raw_columns: Dict[str, Dict[str, Any]]) -> Optional[str]:
        constraint = cls._parse_model_constraint(raw_constraint)
        return cls.process_parsed_constraint(constraint, lambda c: cls.render_model_constraint(c, raw_columns))

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint, raw_columns: Dict[str, Dict[str, Any]]) -> Optional[str]:
        """Render the given constraint as DDL text with column-level quoting if specified in raw_columns."""
        constraint_prefix = f"constraint {constraint.name} " if constraint.name else ""
        # Apply quotes based on raw_columns if specified for primary/foreign keys
        column_list = ", ".join(
            cls.quote(col) if raw_columns.get(col, {}).get("quote") else col
            for col in constraint.columns
        )
        rendered_model_constraint = None

        if constraint.type == ConstraintType.check and constraint.expression:
            rendered_model_constraint = f"{constraint_prefix}check ({constraint.expression})"
        elif constraint.type == ConstraintType.unique:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            rendered_model_constraint = (
                f"{constraint_prefix}unique{constraint_expression} ({column_list})"
            )
        elif constraint.type == ConstraintType.primary_key:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            rendered_model_constraint = (
                f"{constraint_prefix}primary key{constraint_expression} ({column_list})"
            )
        elif constraint.type == ConstraintType.foreign_key:
            if constraint.to and constraint.to_columns:
                to_column_list = ", ".join(
                    cls.quote(col) if raw_columns.get(
                        col, {}).get("quote") else col
                    for col in constraint.to_columns
                )
                rendered_model_constraint = f"{constraint_prefix}foreign key ({column_list}) references {constraint.to} ({to_column_list})"
            elif constraint.expression:
                rendered_model_constraint = f"{constraint_prefix}foreign key ({column_list}) references {constraint.expression}"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            rendered_model_constraint = f"{constraint_prefix}{constraint.expression}"

        return rendered_model_constraint


COLUMNS_EQUAL_SQL = """
with diff_count as (
    SELECT
        1 as id,
        COUNT(*) as num_missing FROM (
            (SELECT {columns} FROM {relation_a} {except_op}
            SELECT {columns} FROM {relation_b})
            UNION ALL
            (SELECT {columns} FROM {relation_b} {except_op}
            SELECT {columns} FROM {relation_a})
        ) as a
), table_a as (
    SELECT COUNT(*) as num_rows FROM {relation_a}
), table_b as (
    SELECT COUNT(*) as num_rows FROM {relation_b}
), row_count_diff as (
    select
        1 as id,
        table_a.num_rows - table_b.num_rows as difference
    from table_a, table_b
)
select
    row_count_diff.difference as row_count_difference,
    diff_count.num_missing as num_mismatched
from row_count_diff
join diff_count
    on row_count_diff.id = diff_count.id
""".strip()
