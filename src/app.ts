export default {
	id: 'transcode-video-operation',
	name: 'Transcode Video Operation',
	icon: 'extension',
	description: 'Transcode input file to HLS streams with multiple quality levels',
	overview: ({ file, folder_id }: { file?: any; folder_id?: string }) => [
		{
			label: 'File',
			text: file,
		},
		{
			label: 'Folder ID',
			text: folder_id,
		}
	],
	options: [
		{
			field: 'file',
			name: 'File',
			type: 'text',
			meta: {
				width: 'half',
				interface: 'input',
				options: {
					placeholder: 'File UUID / File Object',
				},
				note: 'Input file UUID or file object.',
			},
			schema: {
				required: true,
			},
		},
		{
			field: 'folder_id',
			name: 'Folder',
			type: 'uuid',
			meta: {
				width: 'half',
				interface: 'system-folder',
				note: 'Root folder for storing all transcoded files. If not provided, a new folder will be created.',
			},
			schema: {
				required: false,
			},
		},
		{
			field: 'playlist_reference_type',
			name: 'Playlist Reference Type',
			type: 'string',
			meta: {
				width: 'half',
				interface: 'select-dropdown',
				options: {
					choices: [
						{ text: 'Directus File IDs (default)', value: 'id' },
						{ text: 'Filename Disk (custom)', value: 'filename_disk' }
					]
				},
				note: 'How playlists should reference segments: File UUIDs for files in Directus (/assets/:uuid) or Filename Disk for resources stored in a custom location (/stream/:filename_disk.m3u8)'
			},
			schema: {
				default_value: 'id'
			}
		},
		{
			field: 'qualities',
			name: 'Quality Levels',
			type: 'json',
			meta: {
				interface: 'select-multiple-checkbox',
				options: {
					choices: [
						{ text: '240p', value: '240p' },
						{ text: '480p', value: '480p' },
						{ text: '720p', value: '720p' },
						{ text: '1080p', value: '1080p' },
						{ text: '4K', value: '2160p' }
					]
				},
				note: 'Select the quality levels to transcode the video to. The maximum quality level depends on the input video (no upscaling).'
			},
			schema: {
				default_value: ['240p', '480p', '720p', '1080p', '2160p']
			}
		},
		{
			field: 'storage_adapter',
			name: 'Storage Adapter',
			type: 'string',
			meta: {
				width: 'half',
				interface: 'select-radio',
				options: {
					choices: [
						{ text: 'Environment Configuration (First One)', value: 'default' },
						{ text: 'Same as Source File', value: 'source' },
						{ text: 'Other', value: 'custom' }
					]
				},
				note: 'Select the storage adapter where transcoded files physically should be stored.'
			},
			schema: {
				default_value: 'default'
			}
		},
		{
			field: 'target_storage',
			name: 'Target Storage Location',
			type: 'text',
			meta: {
				width: 'half',
				interface: 'input',
				options: {
					placeholder: 'e.g., local, s3, gcs'
				},
				note: 'Specify the storage location name (must match one of your configured STORAGE_LOCATIONS)',
				conditions: [
					{
						name: 'Hide when storage_adapter is not custom',
						rule: {
							_or: [
								{
									storage_adapter: {
										_eq: 'default'
									}
								},
								{
									storage_adapter: {
										_eq: 'source'
									}
								},
								{
									storage_adapter: {
										_null: true
									}
								}
							]
						},
						hidden: true
					}
				]
			},
			schema: {
				required: false
			}
		},
		{
			field: 'performance_divider',
			name: 'Performance Settings',
			type: 'alias',
			meta: {
				width: 'full',
				interface: 'presentation-divider',
				special: ['alias', 'no-data'],
				options: {
					title: 'Performance Settings'
				}
			},
			schema: {}
		},
		{
			field: 'threads',
			name: 'Thread Count',
			type: 'integer',
			meta: {
				width: 'half',
				interface: 'input',
				options: {
					placeholder: '1',
					min: 0,
					step: 1
				},
				note: 'Number of threads to use for transcoding. Use 1 for single-threaded, or 0 to use all available CPU cores. Default: 1'
			},
			schema: {
				default_value: 1
			}
		},
		{
			field: 'nice',
			name: 'Process Priority',
			type: 'integer',
			meta: {
				width: 'half',
				interface: 'input',
				options: {
					placeholder: '19',
					min: 0,
					max: 19,
					step: 1
				},
				note: 'Process priority (nice value) for transcoding. Range: 0 (highest) to 19 (lowest). Keep priority low when transcoding kills your system.'
			},
			schema: {
				default_value: 19,
				required: false
			}
		}
	],
};
