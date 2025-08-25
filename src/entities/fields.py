import asyncio
import json

from ..service import QaseService, TestrailService
from ..support import Logger, Mappings, ConfigManager as Config, Pools


class Fields:
    def __init__(
            self, 
            qase_service: QaseService,
            testrail_service: TestrailService,
            logger: Logger,
            mappings: Mappings,
            config: Config,
            pools: Pools,
    ):
        self.qase = qase_service
        self.testrail = testrail_service
        self.logger = logger
        self.mappings = mappings
        self.config = config
        self.pools = pools

        self.refs_id = None
        self.system_fields = []

        self.map = {}
        self.logger.divider()

    def import_fields(self):
        return asyncio.run(self.import_fields_async())

    async def import_fields_async(self):
        self.logger.log('[Fields] Loading custom fields from Qase')
        qase_custom_fields = await self.pools.qs(self.qase.get_case_custom_fields)
        self.logger.log('[Fields] Loading custom fields from TestRail')
        testrail_custom_fields = await self.pools.tr(self.testrail.get_case_fields)
        self.logger.log('[Fields] Loading system fields from Qase')
        qase_system_fields = await self.pools.qs(self.qase.get_system_fields)
        for field in qase_system_fields:
            self.system_fields.append(field.to_dict())

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._create_types_map())
            tg.create_task(self._create_priorities_map())
            tg.create_task(self._create_result_statuses_map())

        total = len(testrail_custom_fields)
        
        self.logger.log(f'[Fields] Found {str(total)} custom fields')

        fields_to_import = self._get_fields_to_import(testrail_custom_fields)

        i = 0
        self.logger.print_status('Importing custom fields', i, total)
        self.mappings.stats.add_custom_field('testrail', total)
        async with asyncio.TaskGroup() as tg:
            for field in testrail_custom_fields:
                i += 1
                if field['name'] in fields_to_import and field['is_active']:
                    if field['type_id'] in self.mappings.custom_fields_type:
                        tg.create_task(self._create_custom_field(field, qase_custom_fields))
                else:
                    self.logger.log(f'[Fields] Skipping custom field: {field["name"]}')

                if field['type_id'] == 10:
                    self.mappings.step_fields.append(field['name'])
                self.logger.print_status('Importing custom fields', i, total)

        await self._create_refs_field(qase_custom_fields)
        return self.mappings

    def _get_fields_to_import(self, custom_fields):
        self.logger.log('[Fields] Building a map for fields to import')
        fields_to_import = self.config.get('tests.fields')
        if fields_to_import is not None and len(fields_to_import) == 0 and not fields_to_import:
            for field in custom_fields:
                if field['system_name'].startswith('custom_'):
                    fields_to_import.append(field['name'])
        return fields_to_import

    async def _create_custom_field(self, field, qase_fields):
        # Check if field has configurations
        if not field.get('configs') or len(field['configs']) == 0:
            self.logger.log(f'[Fields] Skipping field {field["name"]} - no configurations found')
            return

        self.logger.log(f'[Fields] Processing field {field["name"]} with {len(field["configs"])} configurations')

        # If field has only one configuration and is global, create a single field
        if len(field['configs']) == 1 and field['configs'][0]['context'].get('is_global', False):
            self.logger.log(f'[Fields] Creating global field for {field["name"]}')
            await self._create_single_global_field(field, qase_fields)
            return

        # If field has multiple configurations, create unique fields for each project
        if len(field['configs']) > 1:
            self.logger.log(f'[Fields] Creating project-specific fields for {field["name"]} with {len(field["configs"])} configurations')
            await self._create_project_specific_fields(field, qase_fields)
            return

        # If field has one configuration but is not global, create field for specific projects
        if len(field['configs']) == 1 and not field['configs'][0]['context'].get('is_global', False):
            project_ids = field['configs'][0]['context'].get('project_ids', [])
            self.logger.log(f'[Fields] Creating single project field for {field["name"]} for projects: {project_ids}')
            await self._create_single_project_field(field, qase_fields)
            return

    async def _create_single_global_field(self, field, qase_fields):
        """Create a single field that is enabled for all projects"""
        # Check if field already exists
        if qase_fields and len(qase_fields) > 0:
            for qase_field in qase_fields:
                if (qase_field.title == field['label'] and 
                    self.mappings.custom_fields_type[field['type_id']] == self.mappings.qase_fields_type[qase_field.type.lower()]):
                    self.logger.log(f'[Fields] Global custom field already exists: {field["label"]}')
                    if qase_field.type.lower() in ("selectbox", "multiselect", "radio"):
                        field['qase_values'] = {}
                        values = json.loads(qase_field.value)
                        for value in values:
                            field['qase_values'][value['id']] = value['title']
                    field['qase_id'] = qase_field.id
                    self.mappings.custom_fields[field['name']] = field
                    return

        # Create new global field
        data = self.qase.prepare_custom_field_data(field, self.mappings)
        qase_id = await self.pools.qs(self.qase.create_custom_field, data)
        if qase_id > 0:
            self.logger.log(f'[Fields] Global custom field created: {field["label"]}')
            field['qase_id'] = qase_id
            self.mappings.custom_fields[field['name']] = field
            self.mappings.stats.add_custom_field('qase')
        else:
            self.logger.log(f'[Fields] Failed to create global custom field: {field["label"]}', 'error')

    async def _create_single_project_field(self, field, qase_fields):
        """Create a single field for specific projects"""
        config = field['configs'][0]
        project_ids = config['context'].get('project_ids', [])
        
        # Check if field already exists
        if qase_fields and len(qase_fields) > 0:
            for qase_field in qase_fields:
                if (qase_field.title == field['label'] and 
                    self.mappings.custom_fields_type[field['type_id']] == self.mappings.qase_fields_type[qase_field.type.lower()]):
                    self.logger.log(f'[Fields] Project-specific custom field already exists: {field["label"]}')
                    if qase_field.type.lower() in ("selectbox", "multiselect", "radio"):
                        field['qase_values'] = {}
                        values = json.loads(qase_field.value)
                        for value in values:
                            field['qase_values'][value['id']] = value['title']
                    field['qase_id'] = qase_field.id
                    self.mappings.custom_fields[field['name']] = field
                    return

        # Create new project-specific field
        data = self.qase.prepare_custom_field_data(field, self.mappings)
        qase_id = await self.pools.qs(self.qase.create_custom_field, data)
        if qase_id > 0:
            self.logger.log(f'[Fields] Project-specific custom field created: {field["label"]}')
            field['qase_id'] = qase_id
            self.mappings.custom_fields[field['name']] = field
            self.mappings.stats.add_custom_field('qase')
        else:
            self.logger.log(f'[Fields] Failed to create project-specific custom field: {field["label"]}', 'error')

    async def _create_project_specific_fields(self, field, qase_fields):
        """Create unique fields for each project when field has multiple configurations"""
        # Process each project configuration separately
        for config in field['configs']:
            if not config.get('context', {}).get('project_ids'):
                self.logger.log(f'[Fields] Skipping config for field {field["name"]} - no project_ids found')
                continue
                
            project_ids = config['context']['project_ids']
            self.logger.log(f'[Fields] Processing config for field {field["name"]} with project_ids: {project_ids}')
                
            for project_id in project_ids:
                if project_id not in self.mappings.project_map:
                    self.logger.log(f'[Fields] Skipping project {project_id} for field {field["name"]} - project not in mappings')
                    continue
                    
                project_code = self.mappings.project_map[project_id]
                field_name_with_project = f"{field['name']}_{project_code}"
                
                self.logger.log(f'[Fields] Creating field {field_name_with_project} for project {project_code}')
                
                # Check if field already exists for this project
                field_exists = False
                if qase_fields and len(qase_fields) > 0:
                    for qase_field in qase_fields:
                        if (qase_field.title == field_name_with_project and 
                            self.mappings.custom_fields_type[field['type_id']] == self.mappings.qase_fields_type[qase_field.type.lower()]):
                            self.logger.log(f'[Fields] Custom field already exists for project {project_code}: {field_name_with_project}')
                            if qase_field.type.lower() in ("selectbox", "multiselect", "radio"):
                                field['qase_values'] = {}
                                values = json.loads(qase_field.value)
                                for value in values:
                                    field['qase_values'][value['id']] = value['title']
                            field['qase_id'] = qase_field.id
                            # Store field mapping with project-specific key
                            self.mappings.custom_fields[f"{field['name']}_{project_code}"] = field
                            field_exists = True
                            break
                
                if field_exists:
                    continue
                
                # Create new field for this project
                field_copy = field.copy()
                field_copy['label'] = field_name_with_project
                field_copy['configs'] = [config]  # Use only this project's config
                
                # Store project information for validation
                field_copy['project_id'] = project_id
                field_copy['project_code'] = project_code
                
                # Create project-specific field data
                data = self.qase.prepare_custom_field_data(field_copy, self.mappings)
                
                # Ensure field is only enabled for this specific project
                data['is_enabled_for_all_projects'] = False
                data['projects_codes'] = [project_code]
                
                qase_id = await self.pools.qs(self.qase.create_custom_field, data)
                if qase_id > 0:
                    self.logger.log(f'[Fields] Custom field created for project {project_code}: {field_name_with_project}')
                    field_copy['qase_id'] = qase_id
                    # Store field mapping with project-specific key
                    self.mappings.custom_fields[f"{field['name']}_{project_code}"] = field_copy
                    self.mappings.stats.add_custom_field('qase')
                else:
                    self.logger.log(f'[Fields] Failed to create custom field for project {project_code}: {field_name_with_project}', 'error')
        
    async def _create_refs_field(self, qase_custom_fields):
        if self.config.get('tests.refs.enable'):
            if qase_custom_fields and len(qase_custom_fields) > 0:
                for qase_field in qase_custom_fields:
                    if qase_field.title == 'Refs':
                        self.logger.log('Refs field found')
                        self.mappings.refs_id = qase_field.id
            
            if not self.mappings.refs_id:
                self.logger.log('[Fields] Refs field not found. Creating a new one')
                data = {
                    'title': 'Refs',
                    'entity': 0, # 0 - case, 1 - run, 2 - defect,
                    'type': 2,
                    'is_filterable': True,
                    'is_visible': True,
                    'is_required': False,
                    'is_enabled_for_all_projects': True,
                }
                self.mappings.refs_id = await self.pools.qs(self.qase.create_custom_field, data)

    async def _create_types_map(self):
        self.logger.log('[Fields] Creating types map')

        tr_types = await self.pools.tr(self.testrail.get_case_types)
        qase_types = []

        for field in self.system_fields:
            if field['slug'] == 'type':
                for option in field['options']:
                    qase_types.append(option)

        for tr_type in tr_types:
            self.mappings.types[tr_type['id']] = 1
            for qase_type in qase_types:
                if tr_type['name'].lower() == qase_type['title'].lower():
                    self.mappings.types[tr_type['id']] = int(qase_type['id'])
        
        self.logger.log('[Fields] Types map was created')

    async def _create_priorities_map(self):
        self.logger.log('[Fields] Creating priorities map')

        tr_priorities = await self.pools.tr(self.testrail.get_priorities)
        qase_priorities = []

        for field in self.system_fields:
            if field['slug'] == 'priority':
                for option in field['options']:
                    qase_priorities.append(option)

        default_priority = 1
        for qase_priority in qase_priorities:
            if qase_priority['title'].lower() == 'high':
                default_priority = int(qase_priority['id'])
                self.mappings.default_priority = default_priority
                break

        for tr_priority in tr_priorities:
            self.mappings.priorities[tr_priority['id']] = default_priority
            for qase_priority in qase_priorities:
                if tr_priority['name'].lower() == qase_priority['title'].lower():
                    self.mappings.priorities[tr_priority['id']] = int(qase_priority['id'])

        self.logger.log('[Fields] Priorities map was created')

    async def _create_result_statuses_map(self):
        self.logger.log('[Fields] Creating statuses map')

        tr_statuses = await self.pools.tr(self.testrail.get_result_statuses)
        qase_statuses = []

        for field in self.system_fields:
            if field['slug'] == 'result_status':
                for option in field['options']:
                    qase_statuses.append(option)

        for tr_status in tr_statuses:
            self.mappings.result_statuses[tr_status['id']] = 'skipped'
            for qase_status in qase_statuses:
                if tr_status['label'].lower() == qase_status['title'].lower():
                    self.mappings.result_statuses[tr_status['id']] = qase_status['slug']

        self.logger.log('[Fields] Result statuses map was created')

    def _create_case_statuses_map(self):
        self.logger.log('[Fields] Creating case statuses map')

        tr_statuses = self.testrail.get_case_statuses()
        qase_statuses = []

        for field in self.system_fields:
            if field['slug'] == 'status':
                for option in field['options']:
                    qase_statuses.append(option)

        for tr_status in tr_statuses:
            self.mappings.case_statuses[tr_status['case_status_id']] = 1
            for qase_status in qase_statuses:
                if tr_status['name'].lower() == qase_status['slug'].lower():
                    self.mappings.case_statuses[tr_status['case_status_id']] = qase_status['id']

        self.logger.log('[Fields] Case statuses map was created')
