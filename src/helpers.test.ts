import assert from 'node:assert/strict';
import { test } from 'node:test';
import { appendCapped, generatedFilePattern, sourceStem, timeoutMsFrom } from './helpers';

test('sourceStem drops folders and only the last extension', () => {
	assert.equal(sourceStem('uuid.mp4'), 'uuid');
	assert.equal(sourceStem('videos/2024/clip.v2.mp4'), 'clip.v2');
	assert.equal(sourceStem('v1.2/clip.mp4'), 'clip');
});

test('generatedFilePattern matches outputs but not lookalike sources', () => {
	const generated = generatedFilePattern('clip.v2');
	for (const name of ['clip.v2_240p.m3u8', 'clip.v2_1080p_012.ts', 'clip.v2_master.m3u8', 'clip.v2_thumb.jpg']) {
		assert.equal(generated.test(name), true, name);
	}
	for (const name of ['clip.v2.mp4', 'clip.v2_2.mp4', 'clipXv2_240p.m3u8', 'clip.v2_240p.m3u8.bak']) {
		assert.equal(generated.test(name), false, name);
	}
});

test('appendCapped keeps the end of the output', () => {
	assert.equal(appendCapped('abcd', 'ef', 4), 'cdef');
});

test('timeoutMsFrom ignores unusable values', () => {
	assert.equal(timeoutMsFrom('2', 360), 120_000);
	assert.equal(timeoutMsFrom(undefined, 360), 21_600_000);
	assert.equal(timeoutMsFrom('nope', 1), 60_000);
	assert.equal(timeoutMsFrom('-5', 1), 60_000);
});
