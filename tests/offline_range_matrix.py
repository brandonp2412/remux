#!/usr/bin/env python3
"""Host decoder + HTTP Range fault matrix; never counts as Android playback PASS."""
from __future__ import annotations
import argparse
import array
import hashlib
import http.client
import http.server
import json
import os
import re
import signal
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

class Handler(http.server.BaseHTTPRequestHandler):
    media = b""
    mode = "normal"
    events = []
    stall_injections = 0
    def log_message(self, *_): pass
    def do_GET(self):
        if self.path != "/media.mp4":
            self.send_error(404); return
        raw = self.headers.get("Range")
        m = re.fullmatch(r"bytes=(\d+)-(\d*)", raw or "")
        suffix = re.fullmatch(r"bytes=-(\d+)", raw or "")
        if raw and not (m or suffix):
            self.send_error(400); return
        start = max(0, len(self.media) - int(suffix[1])) if suffix else int(m[1]) if m else 0
        end = min(int(m[2]), len(self.media)-1) if m and m[2] else len(self.media)-1
        if (suffix and int(suffix[1]) == 0) or start >= len(self.media) or end < start:
            self.send_response(416)
            self.send_header("Content-Range", f"bytes */{len(self.media)}")
            self.send_header("Content-Length", "0")
            self.end_headers()
            self.events.append(["unsatisfiable",start,end])
            return
        self.events.append([self.mode,start,end])
        if self.mode == "error":
            self.send_error(503); return
        served_start = 0 if self.mode == "bad_offset" and m and start > 0 else start
        served_end = min(served_start + (end-start) - (1 if self.mode == "bad_end" else 0), len(self.media)-1)
        self.send_response(206 if m or suffix else 200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(served_end-served_start+1))
        if m or suffix: self.send_header("Content-Range", f"bytes {served_start}-{served_end}/{len(self.media)}")
        self.end_headers()
        if self.mode == "reset":
            self.connection.shutdown(2); return
        cutoff = served_start + min(512, (served_end-served_start+1)//2) if self.mode == "early_eof" else served_start + (served_end-served_start+1)//2 if self.mode == "short_body" else served_end+1
        try:
            for pos in range(served_start, cutoff, 4096):
                self.wfile.write(self.media[pos:min(pos+4096,cutoff)])
                self.wfile.flush()
                if self.mode == "stall_once" and pos == start:
                    time.sleep(.8)
                    type(self).stall_injections += 1
                if self.mode == "slow": time.sleep(.02)
                if self.mode == "stall": time.sleep(.12)
        except (BrokenPipeError, ConnectionResetError, OSError): pass
        self.close_connection = True

def run(cmd, timeout=25):
    started=time.monotonic()
    try:
        p = subprocess.run(cmd,capture_output=True,timeout=timeout)
        return {"rc":p.returncode,"bytes":len(p.stdout),
                "sha256":hashlib.sha256(p.stdout).hexdigest(),
                "elapsed_s":round(time.monotonic()-started,3),
                "error":p.stderr.decode(errors="replace")[-400:],"data":p.stdout}
    except subprocess.TimeoutExpired:
        return {"rc":124,"bytes":0,"sha256":"","elapsed_s":round(time.monotonic()-started,3),
                "error":"timeout","data":b""}

def decode(url,seek=0,audio=False):
    cmd=["ffmpeg","-v","error","-nostdin","-rw_timeout","2000000"]
    if seek: cmd += ["-ss",str(seek)]
    cmd += ["-i",url,"-t","1","-map","0:a:0" if audio else "0:v:0"]
    cmd += ["-vn","-f","s16le","-ac","1","-ar","8000","-"] if audio else ["-an","-pix_fmt","rgb24","-f","rawvideo","-"]
    return run(cmd)

def audio_rms_checked(data):
    samples=array.array("h"); samples.frombytes(data)
    assert samples, "no decoded audio samples"
    rms=(sum(sample*sample for sample in samples)/len(samples))**.5
    assert rms>100, f"decoded audio too quiet or silent: RMS={rms:.1f}"
    return round(rms,2)

def inspect(url):
    p=run(["ffprobe","-v","error","-show_entries","stream=codec_type,width,height","-of","json",url])
    return json.loads(p["data"]) if p["rc"]==0 else {}

def controlled_pause_resume(url, output_dir):
    raw=Path(output_dir)/"pause_resume.rgb"
    frame_size=128*72*3
    cmd=["ffmpeg","-v","error","-nostdin","-re","-i",url,"-t","5","-map","0:v:0",
         "-an","-pix_fmt","rgb24","-f","rawvideo","-y",str(raw)]
    p=subprocess.Popen(cmd,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    def wait_for_size(target, timeout):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            if raw.exists() and raw.stat().st_size>=target:
                return raw.stat().st_size
            if p.poll() is not None:
                break
            time.sleep(.05)
        raise AssertionError(f"playback did not advance to {target} bytes")
    try:
        before=wait_for_size(frame_size*3,3)
        os.kill(p.pid,signal.SIGSTOP)
        time.sleep(.1)
        paused=raw.stat().st_size
        time.sleep(.6)
        stable=raw.stat().st_size
        assert stable==paused, f"paused playback advanced {paused}->{stable}"
        os.kill(p.pid,signal.SIGCONT)
        resumed=wait_for_size(paused+frame_size*3,3)
        return {"before_bytes":before,"paused_bytes":paused,"stable_bytes":stable,
                "resumed_bytes":resumed,"pause_s":.6}
    finally:
        if p.poll() is None:
            try: os.kill(p.pid,signal.SIGCONT)
            except ProcessLookupError: pass
            p.terminate()
            try: p.wait(timeout=2)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait(timeout=2)

def validated_range_read(url, start, end):
    request=urllib.request.Request(url,headers={"Range":f"bytes={start}-{end}"})
    with urllib.request.urlopen(request,timeout=3) as response:
        content_range=response.headers["Content-Range"]
        expected_end=min(end,len(Handler.media)-1)
        expected=f"bytes {start}-{expected_end}/{len(Handler.media)}"
        expected_bytes=expected_end-start+1
        assert response.status==206 and content_range==expected, (
            f"mismatched upstream range: requested {start}-{end}, got {content_range}"
        )
        try:
            body=response.read()
        except http.client.IncompleteRead as exc:
            raise AssertionError(
                f"truncated upstream range: expected {expected_bytes} bytes, got {len(exc.partial)}"
            ) from exc
        assert len(body)==expected_bytes, (
            f"truncated upstream range: expected {expected_bytes} bytes, got {len(body)}"
        )
        return {"status":response.status,"content_range":content_range,"bytes":len(body)}

def transient_retry(url, fault_mode, expected_seek, expected_audio):
    Handler.mode=fault_mode; Handler.events=[]
    fault=decode(url,seek=6)
    fault_ranges=list(Handler.events)
    if fault_mode=="reset":
        assert fault["rc"]!=0 and fault["bytes"]==0
    elif fault_mode=="early_eof":
        assert fault["bytes"]<expected_seek["bytes"] or fault["sha256"]!=expected_seek["sha256"]
    Handler.mode="normal"; Handler.events=[]
    recovered=decode(url,seek=6); audio=decode(url,audio=True)
    recovered_ranges=list(Handler.events)
    assert recovered["rc"]==0 and recovered["sha256"]==expected_seek["sha256"]
    assert audio["rc"]==0 and audio["sha256"]==expected_audio["sha256"]
    return {"fault":clean(fault),"fault_ranges":fault_ranges,
            "recovered_seek":clean(recovered),"recovered_audio":clean(audio),
            "recovered_ranges":recovered_ranges}

def decoder_fallback_probe(url):
    primary=run(["ffmpeg","-v","error","-nostdin","-c:v","__missing_decoder__","-i",url,
                 "-t","1","-map","0:v:0","-an","-f","null","-"])
    fallback=decode(url)
    assert primary["rc"]!=0 and fallback["rc"]==0 and fallback["bytes"]>0
    return {"primary":clean(primary),"fallback":clean(fallback)}

def clean(result):
    return {k:v for k,v in result.items() if k!="data"}

def matrix(port):
    with tempfile.TemporaryDirectory(prefix="remux-offline-w1-") as td:
        media=Path(td)/"known_good.mp4"
        gen=run(["ffmpeg","-v","error","-nostdin","-f","lavfi","-i",
                 "testsrc2=size=128x72:rate=12:duration=8",
                 "-f","lavfi","-i","sine=frequency=740:duration=8",
                 "-c:v","mpeg4","-q:v","4","-c:a","aac",
                 "-movflags","+faststart","-y",str(media)],30)
        assert gen["rc"]==0 and media.exists(),gen["error"]
        Handler.media=media.read_bytes()
        srv=http.server.ThreadingHTTPServer(("127.0.0.1",port),Handler)
        thread=threading.Thread(target=srv.serve_forever,daemon=True);thread.start()
        url=f"http://127.0.0.1:{srv.server_port}/media.mp4"
        outcome={"classification":"offline_host_decoder_only","android_playback_verified":False,
                 "fixture_sha256":hashlib.sha256(Handler.media).hexdigest(),"port":srv.server_port,"cases":{}}
        try:
            with urllib.request.urlopen(urllib.request.Request(url,headers={"Range":"bytes=17-48"}),timeout=3) as r:
                assert r.status==206 and r.headers["Content-Range"]==f"bytes 17-48/{len(Handler.media)}"
                assert r.read()==Handler.media[17:49]
            # Suffix and open-ended ranges must return the exact selected bytes.
            for raw_range, start, end in (
                ("bytes=-512", len(Handler.media)-512, len(Handler.media)-1),
                (f"bytes={len(Handler.media)-512}-", len(Handler.media)-512, len(Handler.media)-1),
                (f"bytes=-{len(Handler.media)+100}", 0, len(Handler.media)-1),
            ):
                with urllib.request.urlopen(urllib.request.Request(url, headers={"Range":raw_range}),timeout=3) as response:
                    assert response.status==206
                    assert response.headers["Content-Range"]==f"bytes {start}-{end}/{len(Handler.media)}"
                    assert response.read()==Handler.media[start:end+1]
            for bad_range in [f"bytes={len(Handler.media)}-", "bytes=48-17", "bytes=-0"]:
                try:
                    urllib.request.urlopen(urllib.request.Request(url,headers={"Range":bad_range}),timeout=3)
                    raise AssertionError(f"bad range unexpectedly accepted: {bad_range}")
                except urllib.error.HTTPError as e:
                    assert e.code==416 and e.headers["Content-Range"]==f"bytes */{len(Handler.media)}"
            for mode in ["normal","slow","stall","stall_once","error","reset","early_eof"]:
                Handler.mode=mode;Handler.events=[];Handler.stall_injections=0
                first=decode(url);audio=decode(url,audio=True);seek=decode(url,seek=4);resume=decode(url,seek=6)
                outcome["cases"][mode]={"first":clean(first),"audio":clean(audio),"seek":clean(seek),
                                        "resume":clean(resume),"ranges":list(Handler.events),
                                        "stall_injections":Handler.stall_injections}
                if mode in ("normal","slow","stall","stall_once"):
                    assert first["rc"]==audio["rc"]==seek["rc"]==resume["rc"]==0,mode
                    assert first["bytes"]>=128*72*3*5 and audio["bytes"]>=8000 and resume["bytes"]>=128*72*3*5,mode
                    frame_size=128*72*3
                    frames=[first["data"][i:i+frame_size] for i in range(0,len(first["data"])-frame_size+1,frame_size)]
                    assert len({hashlib.sha256(f).digest() for f in frames})>=3,"decoded video frames do not move"
                    outcome["cases"][mode]["audio_rms_s16"]=audio_rms_checked(audio["data"])
                    assert first["sha256"]!=seek["sha256"],"seek lacks frame change"
                    assert seek["sha256"]!=resume["sha256"],"resume point lacks frame change"
                if mode in ("error","reset"):
                    assert first["rc"]!=0 and audio["rc"]!=0,mode
                if mode=="early_eof":
                    assert any(outcome["cases"]["early_eof"][key]["sha256"] != outcome["cases"]["normal"][key]["sha256"] for key in ("first", "audio", "seek", "resume")), "early EOF was not observable"
            normal=outcome["cases"]["normal"]
            for mode in ("slow","stall","stall_once"):
                assert normal["first"]["sha256"]==outcome["cases"][mode]["first"]["sha256"]
                ranges=outcome["cases"][mode]["ranges"]
                assert any(start==0 for _,start,_ in ranges),f"{mode} lacks startup read"
            for mode in ("slow","stall"):
                ranges=outcome["cases"][mode]["ranges"]
                assert any(start>0 for _,start,_ in ranges),f"{mode} lacks seek/range read"
            assert outcome["cases"]["stall_once"]["stall_injections"]>=4, "stall fault not injected for each playback probe"
            assert outcome["cases"]["error"]["first"]["bytes"]==0
            assert outcome["cases"]["reset"]["first"]["bytes"]==0
            Handler.mode="bad_offset";Handler.events=[]
            try:
                validated_range_read(url,1024,2047)
            except AssertionError as exc:
                assert "mismatched upstream range" in str(exc), str(exc)
                bad_offset={"rejected":True,"error":str(exc),"ranges":list(Handler.events)}
            else:
                raise AssertionError("bad-offset fixture unexpectedly accepted")
            range_faults={"bad_offset":bad_offset}
            for fault_mode, expected_error in (("bad_end", "mismatched upstream range"),
                                               ("short_body", "truncated upstream range")):
                Handler.mode=fault_mode;Handler.events=[]
                try:
                    validated_range_read(url,1024,2047)
                except AssertionError as exc:
                    assert expected_error in str(exc), str(exc)
                    range_faults[fault_mode]={"rejected":True,"error":str(exc),"ranges":list(Handler.events)}
                else:
                    raise AssertionError(f"{fault_mode} fixture unexpectedly accepted")
            Handler.mode="normal";Handler.events=[]
            for silent in (bytes(16000), (bytes([1,0])*8000)):
                try:
                    audio_rms_checked(silent)
                except AssertionError as exc:
                    assert "too quiet or silent" in str(exc)
                else:
                    raise AssertionError("silent PCM falsely passed audio validation")
            outcome["controls"]={
                "pause_resume":controlled_pause_resume(url,td),
                "decoder_fallback":decoder_fallback_probe(url),
                "bad_offset_negative":bad_offset,
                "range_fault_negatives":range_faults,
                "good_offset_positive":validated_range_read(url,1024,2047),
                "eof_clamped_positive":validated_range_read(
                    url,len(Handler.media)-512,len(Handler.media)+511
                ),
                "reset_retry_recovery":transient_retry(
                    url,"reset",normal["resume"],normal["audio"]
                ),
                "early_eof_retry_recovery":transient_retry(
                    url,"early_eof",normal["resume"],normal["audio"]
                ),
            }
            outcome["pass"]=True
        finally:
            srv.shutdown();srv.server_close()
    return outcome

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--port",type=int,default=3101)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    output=matrix(args.port)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(output,indent=2)+"\n")
    print(json.dumps({"pass":output["pass"],"fixture_sha256":output["fixture_sha256"],
                      "modes":list(output["cases"]),"port":output["port"]}))
