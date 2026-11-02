/** Keep the tail: ffmpeg logs a line per progress tick for hours, and errors come last. */
export function appendCapped(current: string, chunk: Buffer | string, maxLength: number): string {
	return (current + chunk.toString()).slice(-maxLength);
}

/** Bare stem of a storage key: `videos/2024/clip.v2.mp4` gives `clip.v2`. */
export function sourceStem(filenameDisk: string): string {
	return filenameDisk.slice(filenameDisk.lastIndexOf('/') + 1).replace(/\.[^.]*$/, '');
}

/** Matches this run's outputs only, so an unrelated `clip_2.mp4` is never picked up. */
export function generatedFilePattern(stem: string): RegExp {
	const escaped = stem.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
	return new RegExp(`^${escaped}_(\\d+p(_\\d+\\.ts|\\.m3u8)|master\\.m3u8|thumb\\.jpg)$`);
}

export function timeoutMsFrom(raw: string | undefined, defaultMinutes: number): number {
	const minutes = Number(raw);
	return (Number.isFinite(minutes) && minutes > 0 ? minutes : defaultMinutes) * 60_000;
}
