module Bosh
  module Director
    module DeploymentPlan
      class PlannerFactory
        def initialize(canonicalizer, deployment_manifest_migrator, deployment_manifest_validator, deployment_repo, event_log, logger)
          @canonicalizer = canonicalizer
          @deployment_manifest_migrator = deployment_manifest_migrator
          @deployment_manifest_validator = deployment_manifest_validator
          @deployment_repo = deployment_repo
          @event_log = event_log
          @logger = logger
        end

        def planner(manifest_hash, cloud_config, plan_options)
          planner = planner_without_vm_binding(manifest_hash, cloud_config, plan_options)
          bind_vms(planner)
        end

        def planner_without_vm_binding(manifest_hash, cloud_config, options)
          manifest_hash = @deployment_manifest_migrator.migrate(manifest_hash)
          @deployment_manifest_validator.validate!(manifest_hash)
          name = manifest_hash['name']

          deployment_model = nil
          @event_log.track('Binding deployment') do
            @logger.info('Binding deployment')
            deployment_model = @deployment_repo.find_or_create_by_name(name)
          end

          attrs = {
            name: name,
            properties: manifest_hash.fetch('properties', {}),
          }
          assemble_without_vm_binding(attrs, manifest_hash, cloud_config, deployment_model, options)
        end

        private

        def deployment_name(manifest_hash)
          @deployment_manifest_validator.validate!(manifest_hash)
          name = manifest_hash['name'] # TODO: error handling, type checking
          @canonicalizer.canonical(name)
        end

        def assemble_without_vm_binding(attrs, manifest_hash, cloud_config, deployment_model, options)
          plan_options = {
            'recreate' => !!options['recreate'],
            'job_states' => options['job_states'] || {},
            'job_rename' => options['job_rename'] || {}
          }
          @logger.info('Creating deployment plan')
          @logger.info("Deployment plan options: #{plan_options.pretty_inspect}")

          parser = DeploymentSpecParser.new(@event_log, @logger)
          parser.parse(attrs, manifest_hash, cloud_config, deployment_model, plan_options)
        end

        def bind_vms(planner)
          stemcell_manager = Api::StemcellManager.new
          cloud = Config.cloud
          blobstore = nil # not used for this assembler purposes
          director_job = nil
          assembler = DeploymentPlan::Assembler.new(
            planner,
            stemcell_manager,
            cloud,
            blobstore,
            @logger,
            @event_log
          )
          @logger.info('Created deployment plan')

          run_prepare_step(assembler)

          DeploymentPlan::Steps::PackageCompileStep.new(
            planner,
            cloud,
            @logger,
            @event_log,
            director_job
          ).perform
          @event_log.begin_stage('Preparing DNS', 1)
          track_and_log('Binding DNS') do
            assembler.bind_dns
          end

          planner
        end

        def run_prepare_step(assembler)
          @event_log.begin_stage('Preparing deployment', 9)
          @logger.info('Preparing deployment')

          track_and_log('Binding releases') do
            assembler.bind_releases
          end

          track_and_log('Binding existing deployment') do
            assembler.bind_existing_deployment
          end

          track_and_log('Binding resource pools') do
            assembler.bind_resource_pools
          end

          track_and_log('Binding stemcells') do
            assembler.bind_stemcells
          end

          track_and_log('Binding templates') do
            assembler.bind_templates
          end

          track_and_log('Binding properties') do
            assembler.bind_properties
          end

          track_and_log('Binding unallocated VMs') do
            assembler.bind_unallocated_vms
          end

          track_and_log('Binding instance networks') do
            assembler.bind_instance_networks
          end
        end

        def track_and_log(message)
          @event_log.track(message) do
            @logger.info(message)
            yield
          end
        end
      end
    end
  end
end
