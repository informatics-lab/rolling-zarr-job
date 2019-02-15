import s3fs
import re
import json


class OffSetS3Map(s3fs.S3Map):
    def __init__(self, *args, temp_chunk_path=None, **kwargs):
        self.temp_chunk_path = temp_chunk_path
        self._offsets = {}
        s3fs.S3Map.__init__(self, *args, **kwargs)
        self._chunk_re = re.compile("^[0-9]+([.][0-9]+)*$")
        
    def _get_offset(self, prefix):
        offset = self._offsets.get(prefix, False)
        if offset is False:
            offset = None
            try:
                path = prefix + '/.zattrs' if prefix else '.zattrs'
                attrs = super().__getitem__(path)
                offset = json.loads(attrs).get('_offset', None)
            except KeyError:
                print(f"key error {prefix}")
                pass
                
            self._offsets[prefix] = offset
        return offset
    
    def _is_chunk(self, path):
        return bool(self._chunk_re.match(path.split('/')[-1]))
    
    def __setitem__(self, path, item):
       
        if not self._is_chunk(path):
            return super().__setitem__(path, item)
        
        if self.temp_chunk_path is None or not path.rsplit('/',1)[0].endswith(self.temp_chunk_path):
            return super().__setitem__(path, item)
        
        try: 
            self.s3.s3_additional_kwargs['Tagging'] = 'type=temp_chunk'
            return super().__setitem__(path, item)
        finally:
            del self.s3.s3_additional_kwargs['Tagging']
                
                
    def __getitem__(self, path):
        origpath = path
        if '/' in path:
            prefix, item = path.rsplit('/',1) 
        else:
            prefix = ''
            item = path
            
        if self._chunk_re.match(item):
            offset = self._get_offset(prefix)
            if offset:
                # Apply offset
                chunks = [int(i) for i in item.split('.')]
                ochunks = [chunk + offset[i] for i,chunk in enumerate(chunks)]
                item = '.'.join(str(v) for v in ochunks)
        
        path = prefix + '/' + item if prefix else item
        return super().__getitem__(path)

class PrintS3Map(s3fs.S3Map):
    def __getitem__(self, path):
        print(f"keep same {path} to {path}")
        return super().__getitem__(path)