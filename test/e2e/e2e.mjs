// End-to-end checks against a real Directus. Run with `npm run test:e2e`.
import fs from 'node:fs';
import path from 'node:path';
import { deflateRawSync, gzipSync } from 'node:zlib';

const B = process.env.E2E_URL;
const UP = path.join(process.env.E2E_DATA, 'uploads');
import { execFileSync } from 'node:child_process';
execFileSync('docker', ['exec', `e2e-${process.env.E2E_NAME}-directus-1`, 'ffmpeg', '-hide_banner', '-loglevel', 'error', '-y', '-f', 'lavfi', '-i', 'testsrc=d=2:s=426x240:r=15', '-f', 'lavfi', '-i', 'sine=d=2', '-pix_fmt', 'yuv420p', '-c:v', 'libx264', '-c:a', 'aac', '-shortest', '/directus/uploads/clip.mp4']);
fs.renameSync(path.join(UP, 'clip.mp4'), path.join(process.env.E2E_DATA, 'clip.mp4'));

let pass = 0;
let fail = 0;
const ok = (name, condition, detail = '') => {
	condition ? pass++ : fail++;
	console.log(`${condition ? 'PASS' : 'FAIL'} ${name}${condition ? '' : `  -> ${JSON.stringify(detail).slice(0, 300)}`}`);
};
const j = async (method, url, body, headers = {}, token = T) => {
	const response = await fetch(B + url, {
		method,
		headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}), ...headers },
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	const text = await response.text();
	let data;
	try {
		data = JSON.parse(text);
	} catch {
		data = text;
	}
	return { s: response.status, d: data };
};
let T = null;
T = (await j('POST', '/auth/login', { email: 'admin@example.com', password: 'e2e-password' })).d.data.access_token;
const mk = async (collection, fields) => {
	const r = await j('POST', '/collections', { collection, schema: {}, meta: {}, fields });
	if (r.s !== 200) console.log('could not create', collection, JSON.stringify(r.d));
};
const uuidPk = { field: 'id', type: 'uuid', schema: { is_primary_key: true }, meta: { special: ['uuid'], hidden: true } };
const intPk = { field: 'id', type: 'integer', schema: { is_primary_key: true, has_auto_increment: true }, meta: {} };

const clip=fs.readFileSync(path.join(process.env.E2E_DATA,'clip.mp4'));
const upTo=async(store,name)=>{const f=new FormData();f.append('storage',store);f.append('file',new Blob([clip],{type:'video/mp4'}),name);const r=await fetch(B+'/files',{method:'POST',headers:{authorization:'Bearer '+T},body:f});return (await r.json()).data};
const folder=(await j('POST','/folders',{name:'hls'})).d.data.id;
const flow=(await j('POST','/flows',{name:'t',status:'active',trigger:'webhook',accountability:'all',options:{method:'POST',async:false,return:'$last'}})).d.data.id;
const op=(await j('POST','/operations',{flow,name:'tc',key:'tc',type:'transcode-video-operation',position_x:1,position_y:1,options:{file:'{{$trigger.body.file}}',folder_id:folder,qualities:['240p'],storage_adapter:'source'}})).d.data.id;
await j('PATCH','/flows/'+flow,{operation:op});
const run=async(file)=>{const r=await j('POST','/flows/trigger/'+flow,{file});return r};
const UP2=path.join(UP,'..','uploads2');
const v=await upTo('abs','src.mp4');const by=await upTo('abs','by.mp4');
fs.mkdirSync(path.join(UP2,'videos'),{recursive:true,mode:0o777});
fs.renameSync(path.join(UP2,v.filename_disk),path.join(UP2,'videos/clip.v2.mp4'));fs.renameSync(path.join(UP2,by.filename_disk),path.join(UP2,'videos/clip.v2_2.mp4'));
await j('PATCH','/files/'+v.id,{filename_disk:'videos/clip.v2.mp4'});await j('PATCH','/files/'+by.id,{filename_disk:'videos/clip.v2_2.mp4'});
const r1=await run(v.id);
ok('tc: absolute root + folder + dotted name transcodes',r1.s==200&&r1.d?.metadata?.availableQualities?.includes(240),r1);
const list=fs.readdirSync(path.join(UP2,'videos'));
ok('tc: outputs use the stem in the source folder',list.includes('clip.v2_master.m3u8')&&list.includes('clip.v2_240p.m3u8'),list);
ok('tc: unrelated clip.v2_2.mp4 untouched',list.includes('clip.v2_2.mp4'),list);
const reg=await j('GET','/files?limit=-1&filter[filename_disk][_eq]=videos/clip.v2_2.mp4');
ok('tc: unrelated file not re-registered',reg.d.data.length==1,reg.d);
const seg=await j('GET','/files?limit=1&filter[filename_download][_eq]=clip.v2_240p.m3u8');
ok('tc: outputs registered with their folder',seg.d.data[0]?.filename_disk=='videos/clip.v2_240p.m3u8',seg.d);
const as=await fetch(B+'/assets/'+seg.d.data[0]?.id,{headers:{authorization:'Bearer '+T}});ok('tc: output asset is served',as.status==200,as.status);
ok('tc: result reports real master path',r1.d?.master?.filename_disk=='videos/clip.v2_master.m3u8',r1.d?.master);
const s=await upTo('s3','cloud.mp4');
ok('tc: private file is not publicly readable',(await fetch(B+'/assets/'+s.id)).status==403);
const r2=await run(s.id);
ok('tc: private cloud source transcodes via AssetsService',r2.s==200&&r2.d?.metadata?.availableQualities?.includes(240),r2);
const f2=new FormData();f2.append('storage','s3');f2.append('file',new Blob(['not a video'],{type:'video/mp4'}),'bad.mp4');
const bad=(await (await fetch(B+'/files',{method:'POST',headers:{authorization:'Bearer '+T},body:f2})).json()).data;
const r3=await run(bad.id);console.log('   corrupt-source result',r3.s,JSON.stringify(r3.d).slice(0,140));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
