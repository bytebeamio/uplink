fn main() {
    let logcat_re = regex::Regex::new(r#"^(\S+ \S+) (\w)/([^(\s]*).+?:\s*(.*)$"#).unwrap();
    let line = "07-25 08:52:02.552 W//apex/com.android.adbd/bin/adbd(  367): type=1400 audit(0.0:21653): avc: denied { search } for comm=73796E6320737663203635 name=\"oem\" dev=\"dm-4\" ino=42 scontext=u:r:adbd:s0 tcontext=u:object_r:oemfs:s0 tclass=dir permissive=0";
    let data = logcat_re.captures(line);
    dbg!(data);
}
