require 'cloud/vsphere/resources/cluster'

module VSphereCloud
  class Resources
    class Datacenter
      include VimSdk

      attr_accessor :config

      def initialize(attrs)
        @client = attrs.fetch(:client)
        @use_sub_folder = attrs.fetch(:use_sub_folder)
        @vm_folder = attrs.fetch(:vm_folder)
        @template_folder = attrs.fetch(:template_folder)
        @name = attrs.fetch(:name)
        @disk_path = attrs.fetch(:disk_path)
        @ephemeral_pattern = attrs.fetch(:ephemeral_pattern)
        @persistent_pattern = attrs.fetch(:persistent_pattern)
        @clusters = attrs.fetch(:clusters)
        @logger = attrs.fetch(:logger)
        @mem_overcommit = attrs.fetch(:mem_overcommit)
      end

      attr_reader :name, :disk_path, :ephemeral_pattern, :persistent_pattern

      def mob
        mob = @client.find_by_inventory_path(name)
        raise "Datacenter: #{name} not found" if mob.nil?
        mob
      end

      def vm_folder
        if @use_sub_folder
          folder_path = [@vm_folder, Bosh::Clouds::Config.uuid].join('/')
          Folder.new(folder_path, @logger, @client, name)
        else
          master_vm_folder
        end
      end

      def vm_path(vm_cid)
        [name, 'vm', vm_folder.path_components, vm_cid].join('/')
      end

      def master_vm_folder
        Folder.new(@vm_folder, @logger, @client, name)
      end

      def template_folder
        if @use_sub_folder
          folder_path = [@template_folder, Bosh::Clouds::Config.uuid].join('/')
          Folder.new(folder_path, @logger, @client, name)
        else
          master_template_folder
        end
      end

      def master_template_folder
        Folder.new(@template_folder, @logger, @client, name)
      end

      def inspect
        "<Datacenter: #{mob} / #{name}>"
      end

      def clusters
        cluster_mobs = get_cluster_mobs

        clusters_properties = @client.cloud_searcher.get_properties(
          cluster_mobs.values, Vim::ClusterComputeResource,
          Cluster::PROPERTIES, :ensure_all => true
        )

        clusters = {}
        @clusters.each do |cluster_name, cluster_config|
          cluster_mob = cluster_mobs[cluster_name]
          raise "Can't find cluster: #{cluster_name}" if cluster_mob.nil?

          cluster_properties = clusters_properties[cluster_mob]
          raise "Can't find properties for cluster: #{cluster_name}" if cluster_properties.nil?

          cluster = Cluster.new(self, @ephemeral_pattern, @persistent_pattern, @mem_overcommit, cluster_config, cluster_properties, @logger, @client)
          clusters[cluster.name] = cluster
        end
        clusters
      end

      def persistent_datastores
        datastores = {}
        clusters.each do |_, cluster|
          cluster.persistent_datastores.each do |_, datastore|
            datastores[datastore.name] = datastore
          end
        end
        datastores
      end

      def pick_persistent_datastore(size)
        weighted_datastores = []
        persistent_datastores.each_value do |datastore|
          if datastore.free_space - size >= DISK_HEADROOM
            weighted_datastores << [datastore, datastore.free_space]
          end
        end

        type = :persistent
        datastores = persistent_datastores.values
        available_datastores = datastores.reject { |datastore| datastore.free_space - size < DISK_HEADROOM }

        @logger.debug("Looking for a #{type} datastore with #{size}MB free space.")
        @logger.debug("All datastores: #{datastores.map(&:debug_info)}")
        @logger.debug("Datastores with enough space: #{available_datastores.map(&:debug_info)}")

        selected_datastore = Util.weighted_random(available_datastores.map { |datastore| [datastore, datastore.free_space] })

        if selected_datastore.nil?
          raise Bosh::Clouds::NoDiskSpace.new(true), "Couldn't find a #{type} datastore with #{size}MB of free space. Found:\n #{datastores.map(&:debug_info).join("\n ")}\n"
        end
        selected_datastore
      end

      private

      def get_cluster_mobs
        cluster_tuples = @client.cloud_searcher.get_managed_objects(
          Vim::ClusterComputeResource, root: mob, include_name: true
        )
        non_clusters = cluster_tuples.reject { |name, _| !@clusters.has_key?(name) }
        Hash[*(non_clusters.flatten)]
      end
    end
  end
end
