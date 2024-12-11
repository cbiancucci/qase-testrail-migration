from .support import ConfigManager, Logger, Mappings, ThrottledThreadPoolExecutor, Pools
from .service import QaseService, TestrailService, QaseScimService
from .entities import Users, Fields, Projects, Suites, Cases, Runs, Milestones, Configurations, Attachments, SharedSteps
from concurrent.futures import ThreadPoolExecutor
import os

class TestRailImporter:
    def __init__(self, config: ConfigManager, logger: Logger) -> None:
        self.pools = Pools(
            qase_pool=ThrottledThreadPoolExecutor(max_workers=8, requests=230, interval=10),
            tr_pool=ThreadPoolExecutor(max_workers=8),
        )

        self.logger = logger
        self.config = config
        self.qase_scim_service = None
        
        self.qase_service = QaseService(config, logger)
        if config.get('qase.scim_token'):
            self.qase_scim_service = QaseScimService(config, logger)

        self.testrail_service = TestrailService(config, logger)

        self.active_project_code = None

        self.mappings = Mappings(self.config.get('users.default'))

    def start(self):
        # Step 1. Build users map
        self.mappings = Users(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.config,
            self.pools,
            self.qase_scim_service,
        ).import_users()

        # Step 2. Import project and build projects map
        self.mappings = Projects(
            self.qase_service, 
            self.testrail_service, 
            self.logger, 
            self.mappings,
            self.config,
            self.pools,
        ).import_projects()

        # Step 3. Import attachments
        self.mappings = Attachments(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.config,
            self.pools,
        ).import_all_attachments()

        # Step 4. Import custom fields
        self.mappings = Fields(
            self.qase_service, 
            self.testrail_service, 
            self.logger, 
            self.mappings,
            self.config,
            self.pools,
        ).import_fields()

        # Step 5. Import projects data in parallel
        #max_workers = min(32, os.cpu_count() + 4)  # Example: use more threads, considering the number of available CPUs
        #print(f"Max workers: {max_workers}")
        #with ThreadPoolExecutor(max_workers) as executor:
        #    futures = []
        #    for project in self.mappings.projects:
        #        # Submit each project import to the thread pool
        #        future = executor.submit(self.import_project_data, project)
        #        futures.append(future)

        #    # Wait for all futures to complete
        #    completed = 0
        #    total_futures = len(futures)
        #    for future in futures:
        #        try:
        #            # This will also re-raise any exceptions caught during execution of the callable
        #            print(f"Launching future: {completed}")
        #            future.result()
        #            completed += 1
        #            remaining = total_futures - completed
        #            print(f"Completed: {completed}/{total_futures}, Remaining: {remaining}")
        #        except Exception as e:
        #            print(f"Chris - An exception occurred: {e}")

        self.import_chris()
        self.mappings.stats.print()
        self.mappings.stats.save(str(self.config.get('prefix')))
        self.mappings.stats.save_xlsx(str(self.config.get('prefix')))

    def import_chris(self):
        print(f"Chris - import_chris")
        completed = 0
        total_projects = len(self.mappings.projects)
        for project in self.mappings.projects:
            print(f"Chris - Launching future: {completed}")
            self.import_project_data(project)
            completed += 1
            remaining = total_projects - completed
            print(f"Chris - Completed: {completed}/{total_projects}, Remaining: {remaining}")

        self.mappings.stats.print()
        self.mappings.stats.save(str(self.config.get('prefix')))
        self.mappings.stats.save_xlsx(str(self.config.get('prefix')))

    def import_project_data(self, project):
        self.logger.print_group(f'Importing project: {project["name"]}'
                                + (' ('
                                   + project['suite_title']
                                   + ')' if 'suite_title' in project else ''))

        self.mappings = Configurations(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.pools,
        ).import_configurations(project)

        self.mappings = Milestones(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
        ).import_milestones(project)

        self.mappings = Suites(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.config,
            self.pools,
        ).import_suites(project)

        Cases(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.config,
            self.pools,
        ).import_cases(project)

        Runs(
            self.qase_service,
            self.testrail_service,
            self.logger,
            self.mappings,
            self.config,
            project,
            self.pools,
        ).import_runs()
