import fs from 'fs';
import path from 'path';
import { exec } from "child_process";

export default {
	id: 'transcode-video-operation',
	handler: async({ file, folder_id, playlist_reference_type = 'id', qualities = ['240p', '480p', '720p', '1080p', '2160p'], threads = 1 }, { env, services, getSchema, logger }) => {
		// if(!env?.RUN_FLOWS) {
		// 	throw {
		// 		message: "Run flows is not allowed"
		// 	};
		// }
		if (!file?.filename_disk) {
			logger?.info("[transcode-video-operation] Input file missing");
			throw new Error("Input file missing");
		}

		if (!folder_id) {
			logger?.info("[transcode-video-operation] folder_id parameter is required");
			throw new Error("folder_id parameter is required");
		}

		const filename = file.filename_disk.split(('.'))[0];
		const extension = file.filename_disk.substr(file.filename_disk.lastIndexOf('.') + 1);

		// STORAGE_LOCATIONS: 'local',
		// STORAGE_LOCAL_DRIVER: 'local',
		// STORAGE_LOCAL_ROOT: './uploads',
		// Get storage location from Directus env config (STORAGE_LOCATIONS is CSV, first one is used)
		const storageLocations = env.STORAGE_LOCATIONS ? env.STORAGE_LOCATIONS.split(',').map(loc => loc.trim()) : [];
		const storageAdapter = storageLocations.length > 0 ? storageLocations[0] : "local";
		const storageLocation = env[`STORAGE_${storageAdapter.toUpperCase()}_ROOT`];
		// outputDir will be set later based on the source file's directory
		let outputDir;
		
		// Function to generate optimized quality options for raw exec commands
		const getQualityOptionsRaw = (isHighBitDepth = false) => {
			// Always use main profile for maximum compatibility
			const profile = 'main';
			
			// Add pixel format conversion for high bit depth videos
			const pixelFormat = isHighBitDepth ? 'format=yuv420p,' : '';
			
			return [
				{ 
					id: 240, 
					options: `-vf "${pixelFormat}scale=w='min(426,iw)':h='min(240,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:a aac -ar 48000 -c:v h264 -profile:v ${profile} -crf 22 -sc_threshold 0 -g 48 -keyint_min 48 -hls_time 4 -hls_playlist_type vod -b:v 400k -maxrate 428k -bufsize 600k -b:a 64k -hls_segment_filename ${outputDir}/${filename}_240p_%03d.ts ${outputDir}/${filename}_240p.m3u8`
				},
				{ 
					id: 480, 
					options: `-vf "${pixelFormat}scale=w='min(854,iw)':h='min(480,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:a aac -ar 48000 -c:v h264 -profile:v ${profile} -crf 20 -sc_threshold 0 -g 48 -keyint_min 48 -hls_time 4 -hls_playlist_type vod -b:v 1400k -maxrate 1498k -bufsize 2100k -b:a 128k -hls_segment_filename ${outputDir}/${filename}_480p_%03d.ts ${outputDir}/${filename}_480p.m3u8`
				},
				{ 
					id: 720, 
					options: `-vf "${pixelFormat}scale=w='min(1280,iw)':h='min(720,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:a aac -ar 48000 -c:v h264 -profile:v ${profile} -crf 20 -sc_threshold 0 -g 48 -keyint_min 48 -hls_time 4 -hls_playlist_type vod -b:v 2800k -maxrate 2996k -bufsize 4200k -b:a 128k -hls_segment_filename ${outputDir}/${filename}_720p_%03d.ts ${outputDir}/${filename}_720p.m3u8`
				},
				{ 
					id: 1080, 
					options: `-vf "${pixelFormat}scale=w='min(1920,iw)':h='min(1080,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:a aac -ar 48000 -c:v h264 -profile:v ${profile} -crf 20 -sc_threshold 0 -g 48 -keyint_min 48 -hls_time 4 -hls_playlist_type vod -b:v 5000k -maxrate 5350k -bufsize 7500k -b:a 192k -hls_segment_filename ${outputDir}/${filename}_1080p_%03d.ts ${outputDir}/${filename}_1080p.m3u8`
				},
				{ 
					id: 2160, 
					options: `-vf "${pixelFormat}scale=w='min(3840,iw)':h='min(2160,ih)':force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2" -c:a aac -ar 48000 -c:v h264 -profile:v ${profile} -crf 20 -sc_threshold 0 -g 48 -keyint_min 48 -hls_time 4 -hls_playlist_type vod -b:v 20000k -maxrate 21400k -bufsize 30000k -b:a 192k -hls_segment_filename ${outputDir}/${filename}_2160p_%03d.ts ${outputDir}/${filename}_2160p.m3u8`
				}
			];
		};
		const hlsFolderId = folder_id;
		
		const resolveStorage = (location) => {
			const envKey = `STORAGE_${location.toUpperCase()}_ROOT`;
			const envValue = env[envKey];

			if (envValue) {
				return envValue;
			} else {
				logger?.warn(`[transcode-video-operation] (${filename}) No storage found for location <%s>`, location);
				return null;
			}
		}

		const readFiles = (linkFilePath) => {
			const data = fs.readdirSync(linkFilePath, 'utf-8').filter(fn => fn.startsWith(filename));
			return data;
		}

		// Get video metadata (dimensions, duration)
		const getVideoMetadata = async (inputFile) => {
			return new Promise((resolve, reject) => {
				// Get width, height, duration, and rotation
				exec(`ffprobe -v error -select_streams v:0 -show_entries stream=width,height:format=duration -of json ${inputFile}`, 
					(error, stdout) => {
						if (error) {
							logger?.error(`[transcode-video-operation] (${filename}) Error getting video metadata:`, error);
							reject(error);
							return;
						}
						
						try {
							const data = JSON.parse(stdout);
							const stream = data.streams?.[0];
							const format = data.format;
							
							if (!stream || !stream.width || !stream.height) {
								reject(new Error('Could not get video dimensions'));
								return;
							}
							
							const width = parseInt(stream.width);
							const height = parseInt(stream.height);
							const duration = format?.duration ? Math.floor(parseFloat(format.duration) * 1000) : 0;
							const isVertical = height > width;
							
							resolve({ width, height, isVertical, duration });
						} catch (parseError) {
							logger?.error(`[transcode-video-operation] (${filename}) Error parsing metadata:`, parseError);
							reject(parseError);
						}
					});
			});
		};

		// Extract thumbnail at 1 second
		const extractThumbnail = async (inputFile, outputPath) => {
			return new Promise((resolve, reject) => {
				exec(`ffmpeg -y -i ${inputFile} -ss 1 -vframes 1 -q:v 2 ${outputPath}`, (error) => {
					if (error) {
						logger?.error(`[transcode-video-operation] (${filename}) Error extracting thumbnail:`, error);
						reject(error);
					} else {
						resolve(outputPath);
					}
				});
			});
		};

		// Get image metadata (dimensions)
		const getImageMetadata = async (imagePath) => {
			return new Promise((resolve, reject) => {
				exec(`ffprobe -v error -select_streams v:0 -show_entries stream=width,height -of json ${imagePath}`, 
					(error, stdout) => {
						if (error) {
							logger?.error(`[transcode-video-operation] (${filename}) Error getting image metadata:`, error);
							reject(error);
							return;
						}
						
						try {
							const data = JSON.parse(stdout);
							const stream = data.streams?.[0];
							
							if (!stream || !stream.width || !stream.height) {
								reject(new Error('Could not get image dimensions'));
								return;
							}
							
							const width = parseInt(stream.width);
							const height = parseInt(stream.height);
							
							resolve({ width, height });
						} catch (parseError) {
							logger?.error(`[transcode-video-operation] (${filename}) Error parsing image metadata:`, parseError);
							reject(parseError);
						}
					});
			});
		};

		// Create folder in Directus if it doesn't exist
		const ensureFolder = async (folderName, parentFolderId = null) => {
			try {
				const { FoldersService } = services;
				const foldersService = new FoldersService({
					schema: await getSchema(),
				});

				// Build filter to find existing folder
				const filter = { name: { _eq: folderName } };
				if (parentFolderId) {
					filter.parent = { _eq: parentFolderId };
				}

				// Try to find existing folder
				const existingFolders = await foldersService.readByQuery({
					filter: filter
				});

				if (existingFolders && existingFolders.length > 0) {
					const folderId = existingFolders[0]?.id || existingFolders[0]?.data?.id || existingFolders[0];
					logger?.info(`[transcode-video-operation] (${filename}) Found existing folder: ${folderId}`);
					return folderId;
				}

				// Create new folder
				const folderData = { name: folderName };
				if (parentFolderId) {
					folderData.parent = parentFolderId;
					logger?.info(`[transcode-video-operation] (${filename}) Creating folder "${folderName}" with parent ${parentFolderId}`);
				} else {
					logger?.info(`[transcode-video-operation] (${filename}) Creating folder "${folderName}" at root`);
				}

				const newFolder = await foldersService.createOne(folderData);
				logger?.debug(`[transcode-video-operation] (${filename}) Folder creation response:`, JSON.stringify(newFolder, null, 2));
				
				// Try different possible response structures
				const folderId = newFolder?.id || newFolder?.data?.id || (typeof newFolder === 'string' ? newFolder : null);
				
				if (!folderId) {
					throw new Error(`Failed to get folder ID from response: ${JSON.stringify(newFolder)}`);
				}
				
				logger?.info(`[transcode-video-operation] (${filename}) Created folder with ID: ${folderId}`);
				return folderId;
			} catch (error) {
				logger?.error(`[transcode-video-operation] (${filename}) Error creating folder:`, error);
				throw error;
			}
		};

		// Create file record in Directus (file is already in storage, no upload needed)
		const uploadFileToDirectus = async (filePath, folderId = null, options = {}) => {
			try {
				const { FilesService } = services;
				const filesService = new FilesService({
					schema: await getSchema(),
				});

				const fileName = path.basename(filePath);
				const extension = fileName.substr(fileName.lastIndexOf('.') + 1);
				const fileSizeInBytes = fs.statSync(filePath).size;
				
				const types = {
					ts: "video/mp2t",
					mp4: "video/mp4",
					jpeg: "image/jpeg",
					jpg: "image/jpeg",
					m3u8: "application/x-mpegurl"
				};

				// Read file buffer (FilesService might need it even though file exists in storage)
				const fileBuffer = fs.readFileSync(filePath);
				const fileStream = fs.createReadStream(filePath);

				// Prepare file data
				const fileData = {
					storage: storageAdapter,
					filename_disk: fileName,
					filename_download: fileName,
					title: fileName,
					type: options.mimetype || types[extension] || 'application/octet-stream',
					filesize: fileSizeInBytes
				};

				// Add width and height if provided
				if (options.width !== undefined && options.width !== null) {
					fileData.width = options.width;
				}
				if (options.height !== undefined && options.height !== null) {
					fileData.height = options.height;
				}

				// Add folder if provided
				if (folderId) {
					fileData.folder = folderId;
				}

				// Create file record in Directus
				// Note: Even though file exists in storage, we may need to provide it to create the record
				const fileRecord = await filesService.createOne(
					fileData,
					{
						file: fileStream,
						title: fileName
					}
				);


				// Try different possible response structures
				const fileId = fileRecord?.id || fileRecord?.data?.id || (typeof fileRecord === 'string' ? fileRecord : null);
				
				if (!fileId) {
					throw new Error(`Failed to get file ID from response. Response: ${JSON.stringify(fileRecord)}`);
				}

				return fileId;
			} catch (error) {
				logger?.error(`[transcode-video-operation] (${filename}) Error creating file record for ${filePath}:`, error);
				throw error;
			}
		};

		// Parse m3u8 file and replace filenames with file IDs or filename_disk
		const rebuildPlaylist = (playlistPath, fileIdMap, useFilenameDisk = false) => {
			let content = fs.readFileSync(playlistPath, 'utf-8');
			const lines = content.split('\n');
			const newLines = [];

			for (const line of lines) {
				if (line.startsWith('#') || line.trim() === '') {
					// Keep comments and empty lines as-is
					newLines.push(line);
				} else {
					// Replace filename with file ID or filename_disk (relative path, no /assets/ prefix)
					let filename = line.trim();
					// Strip /assets/ prefix if present
					if (filename.startsWith('/assets/')) {
						filename = filename.substring('/assets/'.length);
					}
					// Also try with just the basename in case path is included
					const basename = path.basename(filename);
					const fileId = fileIdMap[filename] || fileIdMap[basename];
					
					if (fileId) {
						if (useFilenameDisk) {
							// Use filename_disk (the original filename)
							newLines.push(filename);
						} else {
							// Use file ID (relative to playlist location)
							newLines.push(fileId);
						}
					} else {
						// Keep original if not found (shouldn't happen)
						logger?.warn(`[transcode-video-operation] (${filename}) File ID not found for: ${filename} (tried: ${filename}, ${basename})`);
						newLines.push(line);
					}
				}
			}

			return newLines.join('\n');
		};

		function checkFFmpegAvailable() {
			return new Promise((resolve, reject) => {
				exec('which ffmpeg', (error, stdout, stderr) => {
					if (error || !stdout.trim()) {
						reject(new Error('FFmpeg is not installed or not found in PATH. Please install ffmpeg.'));
						return;
					}
					resolve();
				});
			});
		}

		function ffmpegRawSync(inputFile, quality) {
			return new Promise((resolve, reject) => {
				exec(`ffmpeg -y -i ${inputFile} -threads ${validatedThreads} ${quality.options}`, (error, stdout, stderr) => {
					if (error) {
						logger?.error(`[transcode-video-operation] (${filename}) Error occured for quality: %s`, quality.id);
						logger?.error(error.message);
						logger?.error(`stdout: ${stdout}`);
						logger?.error(`stderr: ${stderr}`);
						reject(new Error(`FFmpeg transcoding failed for quality ${quality.id}p: ${error.message}. stderr: ${stderr}`));
						return;
					}

					// Check stderr for common error messages even if exec didn't report an error
					if (stderr && (stderr.includes('not found') || stderr.includes('command not found'))) {
						logger?.error(`[transcode-video-operation] (${filename}) FFmpeg not found in stderr for quality: %s`, quality.id);
						logger?.error(`stderr: ${stderr}`);
						reject(new Error(`FFmpeg command not found. stderr: ${stderr}`));
						return;
					}

					// fs.copyFileSync('./playlist.m3u8', `${outputPath}/playlist.m3u8`);
			
					// const videoUrl = `http://localhost:8000/uploads/hls-videos/${videoId}/playlist.m3u8`
			
					// try{
					// 	storelink(videoUrl);
					// } catch(error){
					// 	logger?.error(`[ERROR] error while storing video URL: ${error}`);
					// 	res.json({"error": "Error while processing your file. Please try again."})
					// }
					logger?.info(`[transcode-video-operation] (${filename}) Transcoding finished for quality: %s`, quality.id);
					resolve(stdout.trim());
				})
			})
		}
	
		/* Start of the script */
		logger?.info(`[transcode-video-operation] (${filename}) Operation started`);
		
		// Ensure threads is a number (may come as string from form input)
		// 0 means use all available cores, 1+ means use that many threads
		const threadCount = threads !== undefined && threads !== null ? parseInt(threads, 10) : 1;
		const validatedThreads = (isNaN(threadCount) || threadCount < 0) ? 1 : threadCount;

		const storagePath = resolveStorage(file.storage);
		if (!storagePath) {
			return {
				error: `No storage found for location <${file.storage}>`
			}
		}
		
		// Construct file path - use process.env.PWD or fallback to /directus
		const basePath = process.env.PWD || '/directus';
		const filePath = path.join(basePath, `${storagePath}/${file.filename_disk}`);
		
		// Verify source file exists
		if (!fs.existsSync(filePath)) {
			logger?.error(`[transcode-video-operation] (${filename}) Source file not found: %s`, filePath);
			return {
				error: `Source file not found: ${filePath}`
			};
		}
		
		// Set output directory to the same directory as the source file
		outputDir = path.dirname(filePath);
		
		logger?.info(`[transcode-video-operation] (${filename}) File to be transcoded: %s`, filePath)
		logger?.info(`[transcode-video-operation] (${filename}) Output directory: %s`, outputDir)
		
		// Ensure the output directory exists
		if (!fs.existsSync(outputDir)) {
			fs.mkdirSync(outputDir, {recursive: true});
			logger?.info(`[transcode-video-operation] (${filename}) Folder created`)
		}
		
		// Check if ffmpeg is available before starting any transcoding
		try {
			await checkFFmpegAvailable();
			logger?.info(`[transcode-video-operation] (${filename}) FFmpeg is available`);
		} catch (error) {
			logger?.error(`[transcode-video-operation] (${filename}) FFmpeg check failed: %s`, error.message);
			throw error;
		}

		// Check if input is 10-bit by examining the video stream
		const isHighBitDepth = await new Promise((resolve, reject) => {
				exec(`ffprobe -v error -select_streams v:0 -show_entries stream=pix_fmt -of json ${filePath}`, 
					(error, stdout) => {
						if (error) {
							logger?.warn(`[transcode-video-operation] (${filename}) Error checking bit depth, assuming 8-bit: %s`, error.message);
							resolve(false); // Default to false if check fails
							return;
						}
						try {
							const data = JSON.parse(stdout);
							const pixFmt = data.streams?.[0]?.pix_fmt;
							// Check if pixel format indicates 10-bit (e.g., yuv420p10le)
							resolve(pixFmt?.includes('10') || false);
						} catch (parseError) {
							logger?.warn(`[transcode-video-operation] (${filename}) Error parsing bit depth check, assuming 8-bit`);
							resolve(false);
						}
					});
		});

		if (isHighBitDepth) {
			logger?.info(`[transcode-video-operation] (${filename}) High bit depth detected, will convert to yuv420p`);
		}

		// Get video metadata early to determine source resolution and prevent upscaling
		logger?.info(`[transcode-video-operation] (${filename}) Getting source video metadata...`);
		const sourceMetadata = await getVideoMetadata(filePath).catch(error => {
			logger?.error(`[transcode-video-operation] (${filename}) Error getting source metadata:`, error);
			// If we can't get metadata, allow all qualities (fallback behavior)
			return { width: 99999, height: 99999, isVertical: false, duration: 0 };
		});
		
		const sourceHeight = sourceMetadata.height;
		logger?.info(`[transcode-video-operation] (${filename}) Source video resolution: ${sourceMetadata.width}x${sourceHeight}`);

		// Get optimized quality options
		const allQualitiesRaw = getQualityOptionsRaw(isHighBitDepth);
		
		// Filter qualities based on user selection (default: all)
		// Handle cases where qualities might be undefined, null, or not an array
		// Tags interface returns strings with "p" suffix (e.g., "240p"), so default is also strings with "p"
		let selectedQualities = ['240p', '480p', '720p', '1080p', '2160p']; // Default: all qualities
		if (qualities) {
			if (Array.isArray(qualities)) {
				selectedQualities = qualities;
			} else if (typeof qualities === 'string') {
				// Try to parse as JSON if it's a string
				try {
					selectedQualities = JSON.parse(qualities);
				} catch (e) {
					logger?.warn(`[transcode-video-operation] (${filename}) Could not parse qualities, using all:`, e);
				}
			}
		}
		
		// Convert to numbers (tags interface returns strings)
		// Strip "p" suffix if present (e.g., "240p" -> 240)
		selectedQualities = selectedQualities
			.map(q => {
				if (typeof q === 'string') {
					// Remove "p" suffix if present
					const cleaned = q.replace(/p$/i, '');
					return parseInt(cleaned, 10);
				}
				return q;
			})
			.filter(q => !isNaN(q));
		
		// Map quality IDs to their target heights
		const qualityHeights = {
			240: 240,
			480: 480,
			720: 720,
			1080: 1080,
			2160: 2160
		};
		
		// Filter qualities: first by user selection, then by source resolution (prevent upscaling)
		let qualitiesRaw = allQualitiesRaw.filter(quality => selectedQualities.includes(quality.id));
		
		// Filter out qualities that would require upscaling
		const qualitiesBeforeFilter = qualitiesRaw.length;
		qualitiesRaw = qualitiesRaw.filter(quality => {
			const targetHeight = qualityHeights[quality.id];
			if (targetHeight && targetHeight > sourceHeight) {
				logger?.info(`[transcode-video-operation] (${filename}) Skipping ${quality.id}p (target: ${targetHeight}px, source: ${sourceHeight}px) to prevent upscaling`);
				return false;
			}
			return true;
		});
		
		if (qualitiesBeforeFilter > qualitiesRaw.length) {
			logger?.info(`[transcode-video-operation] (${filename}) Filtered out ${qualitiesBeforeFilter - qualitiesRaw.length} quality level(s) that would require upscaling`);
		}
		
		logger?.info(`[transcode-video-operation] (${filename}) Selected qualities: ${selectedQualities.join(', ')}`);
		logger?.info(`[transcode-video-operation] (${filename}) Will transcode ${qualitiesRaw.length} quality levels`);
		
		if (qualitiesRaw.length === 0) {
			return {
				error: 'No quality levels selected for transcoding'
			};
		}
		
		// Check if transcoded files already exist
		const existingFiles = readFiles(outputDir);
		const hasFiles = existingFiles.some(file => file.includes('_240p') || file.includes('_480p') || file.includes('_720p') || file.includes('_1080p') || file.includes('_2160p'));
		
		if (!hasFiles) {
			logger?.info(`[transcode-video-operation] (${filename}) No existing files found, starting transcoding...`);
			// Process qualities sequentially to catch errors on the first quality level
			for (const quality of qualitiesRaw) {
				try {
					logger?.info(`[transcode-video-operation] (${filename}) Starting transcoding for quality: %sp`, quality.id);
					await ffmpegRawSync(filePath, quality);
					logger?.info(`[transcode-video-operation] (${filename}) Successfully transcoded quality: %sp`, quality.id);
				} catch (error) {
					logger?.error(`[transcode-video-operation] (${filename}) Failed to transcode quality %sp:`, quality.id, error);
					throw error; // Re-throw to abort the operation
				}
			}
			logger?.info(`[transcode-video-operation] (${filename}) All qualities transcoded successfully`);
		} else {
			logger?.info(`[transcode-video-operation] (${filename}) Transcoded files already exist, skipping transcoding`);
		}

		// Generate master playlist dynamically based on available quality files
		const m3u8Content = ['#EXTM3U', '#EXT-X-VERSION:3'];
		
		// Add available quality streams (only if the file exists)
		for (const quality of qualitiesRaw) {
			const qualityFile = `${outputDir}/${filename}_${quality.id}p.m3u8`;
			if (fs.existsSync(qualityFile)) {
				switch (quality.id) {
					case 240:
						m3u8Content.push('#EXT-X-STREAM-INF:BANDWIDTH=400000,RESOLUTION=426x240', `${filename}_240p.m3u8`);
						break;
					case 480:
						m3u8Content.push('#EXT-X-STREAM-INF:BANDWIDTH=1400000,RESOLUTION=854x480', `${filename}_480p.m3u8`);
						break;
					case 720:
						m3u8Content.push('#EXT-X-STREAM-INF:BANDWIDTH=2800000,RESOLUTION=1280x720', `${filename}_720p.m3u8`);
						break;
					case 1080:
						m3u8Content.push('#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080', `${filename}_1080p.m3u8`);
						break;
					case 2160:
						m3u8Content.push('#EXT-X-STREAM-INF:BANDWIDTH=20000000,RESOLUTION=3840x2160', `${filename}_2160p.m3u8`);
						break;
				}
			}
		}

		fs.writeFileSync(`${outputDir}/${filename}_playlist.m3u8`, m3u8Content.join('\n'));
		logger?.info(`[transcode-video-operation] (${filename}) Master playlist created: ${filename}_playlist.m3u8`);

		// Use metadata we already retrieved earlier (sourceMetadata)
		const metadata = sourceMetadata;

		// Extract thumbnail
		const thumbnailPath = `${outputDir}/${filename}_thumb.jpg`;
		let thumbnailId = null;
		try {
			await extractThumbnail(filePath, thumbnailPath);
			logger?.info(`[transcode-video-operation] (${filename}) Thumbnail extracted`);
		} catch (error) {
			logger?.error(`[transcode-video-operation] (${filename}) Error extracting thumbnail:`, error);
		}

		// Create virtual folder for this file's transcoded assets
		// Use folder_id as parent and filename as the folder name
		logger?.info(`[transcode-video-operation] (${filename}) Creating virtual folder...`);
		const targetFolderId = await ensureFolder(filename, folder_id);
		logger?.info(`[transcode-video-operation] (${filename}) Created/using folder: ${targetFolderId}`);

		// Get all files to upload
		const files = readFiles(outputDir);
		logger?.info(`[transcode-video-operation] (${filename}) Found ${files.length} files to upload`);

		// Upload all files and create filename -> file ID map
		const fileIdMap = {};
		const uploadedFiles = [];

		// Upload thumbnail first if it exists
		if (fs.existsSync(thumbnailPath)) {
			try {
				// Get thumbnail dimensions
				let thumbnailWidth = null;
				let thumbnailHeight = null;
				try {
					const imageMetadata = await getImageMetadata(thumbnailPath);
					thumbnailWidth = imageMetadata.width;
					thumbnailHeight = imageMetadata.height;
					logger?.info(`[transcode-video-operation] (${filename}) Thumbnail dimensions: ${thumbnailWidth}x${thumbnailHeight}`);
				} catch (error) {
					logger?.warn(`[transcode-video-operation] (${filename}) Could not get thumbnail dimensions:`, error);
				}

				// Upload thumbnail with metadata
				thumbnailId = await uploadFileToDirectus(thumbnailPath, targetFolderId, {
					mimetype: 'image/jpeg',
					width: thumbnailWidth,
					height: thumbnailHeight
				});
				fileIdMap[path.basename(thumbnailPath)] = thumbnailId;
				uploadedFiles.push({ filename_disk: path.basename(thumbnailPath), id: thumbnailId });
				logger?.info(`[transcode-video-operation] (${filename}) Thumbnail uploaded: ${thumbnailId}`);
			} catch (error) {
				logger?.error(`[transcode-video-operation] (${filename}) Error uploading thumbnail:`, error);
			}
		}

		// Upload all other files (excluding source video, thumbnails, and master playlist)
		for (const currentFile of files) {
			if (currentFile === file.filename_disk || 
				currentFile.endsWith('_thumb.jpg') || 
				currentFile === `${filename}_playlist.m3u8`) {
				continue; // Skip source file, thumbnail, and master playlist (master will be uploaded after rebuilding)
			}

			const filePathToUpload = `${outputDir}/${currentFile}`;
			try {
				const fileId = await uploadFileToDirectus(filePathToUpload, targetFolderId);
				fileIdMap[currentFile] = fileId;
				uploadedFiles.push({ filename_disk: currentFile, id: fileId });
			} catch (error) {
				logger?.error(`[transcode-video-operation] (${filename}) Error uploading ${currentFile}:`, error);
			}
		}
		
		logger?.info(`[transcode-video-operation] (${filename}) All files uploaded to Directus: ${uploadedFiles.length} files total`);

		// Determine reference type for playlists
		const useFilenameDisk = playlist_reference_type === 'filename_disk';
		const referenceTypeLabel = useFilenameDisk ? 'filename_disk' : 'file IDs';
		logger?.info(`[transcode-video-operation] (${filename}) Rebuilding playlists with ${referenceTypeLabel}...`);
		
		// Rebuild quality playlists
		for (const quality of qualitiesRaw) {
			const qualityPlaylistPath = `${outputDir}/${filename}_${quality.id}p.m3u8`;
			if (fs.existsSync(qualityPlaylistPath)) {
				const rebuiltContent = rebuildPlaylist(qualityPlaylistPath, fileIdMap, useFilenameDisk);
				fs.writeFileSync(qualityPlaylistPath, rebuiltContent);
				
				// Re-upload the rebuilt playlist
				try {
					const playlistId = await uploadFileToDirectus(qualityPlaylistPath, targetFolderId);
					fileIdMap[path.basename(qualityPlaylistPath)] = playlistId;
					uploadedFiles.push({ filename_disk: path.basename(qualityPlaylistPath), id: playlistId });
					logger?.info(`[transcode-video-operation] (${filename}) Rebuilt and uploaded ${quality.id}p playlist: ${playlistId}`);
				} catch (error) {
					logger?.error(`[transcode-video-operation] (${filename}) Error re-uploading ${quality.id}p playlist:`, error);
				}
			}
		}

		// Rebuild master playlist
		const masterPlaylistPath = `${outputDir}/${filename}_playlist.m3u8`;
		const masterContent = fs.readFileSync(masterPlaylistPath, 'utf-8');
		const masterLines = masterContent.split('\n');
		const newMasterLines = [];

		for (const line of masterLines) {
			if (line.startsWith('#') || line.trim() === '') {
				newMasterLines.push(line);
			} else {
				// Replace quality playlist filename with file ID or filename_disk
				let playlistFilename = line.trim();
				// Strip /assets/ prefix if present
				if (playlistFilename.startsWith('/assets/')) {
					playlistFilename = playlistFilename.substring('/assets/'.length);
				}
				// Also try with just the basename
				const basename = path.basename(playlistFilename);
				const playlistId = fileIdMap[playlistFilename] || fileIdMap[basename];
				if (playlistId) {
					if (useFilenameDisk) {
						// Use filename_disk (the original filename)
						newMasterLines.push(playlistFilename);
					} else {
						// Use file ID (relative to master playlist location)
						newMasterLines.push(playlistId);
					}
					} else {
						logger?.warn(`[transcode-video-operation] (${filename}) File ID not found for playlist: ${playlistFilename} (tried: ${playlistFilename}, ${basename})`);
						newMasterLines.push(line);
					}
			}
		}

		fs.writeFileSync(masterPlaylistPath, newMasterLines.join('\n'));
		
		// Upload master playlist
		let masterId = null;
		try {
			masterId = await uploadFileToDirectus(masterPlaylistPath, targetFolderId);
			logger?.info(`[transcode-video-operation] (${filename}) Master playlist uploaded: ${masterId}`);
		} catch (error) {
			logger?.error(`[transcode-video-operation] (${filename}) Error uploading master playlist:`, error);
		}

		// Determine available qualities
		const availableQualities = [];
		for (const quality of qualitiesRaw) {
			const qualityFile = `${outputDir}/${filename}_${quality.id}p.m3u8`;
			if (fs.existsSync(qualityFile)) {
				availableQualities.push(quality.id);
			}
		}

		return {
			master: { id: masterId, filename_disk: `${filename}_playlist.m3u8` },
			metadata: {
				availableQualities,
				dimensions: {
					width: metadata.width,
					height: metadata.height,
					isVertical: metadata.isVertical
				},
				duration: metadata.duration,
				thumbnail: thumbnailId
			},
			files: uploadedFiles
		};
	}
};