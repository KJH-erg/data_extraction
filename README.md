## 커스텀한 추출을 요청할 경우 사용하는 추출용 코드
* 참고링크
https://crowdworksinc.atlassian.net/wiki/spaces/DET/pages/1542750987

### 1. DB 및 gcs 서비스파일 환경변수 설정
* DB정보
    ~~~
    아래 문서 참고
    https://crowdworksinc.atlassian.net/wiki/spaces/DET/pages/1520107521/login%2Binfo
    ~~~
* gcs 서비스 파일
    * 서비스 파일 받은 후 특정 위치에 놓기
    ~~~
    # server envs
    export GOOGLE_APPLICATION_CREDENTIALS="<path>/<GCS 서비스 계정>.json"
    ~~~
    * 환경변수 영구 적용하는 방향으로
### 2. 실행 커맨드
* 커맨드
    ~~~
    # python3 sample_thr.py {디버그 구분} {시작인덱스} {추출건수} {조직번호} {프로젝트번호} {프로젝트 구분 문자열} {티켓코드} {메일}
    python sample_thr.py file 0 10 81 4941 designovel designovel csv admin@gmail.com
    ~~~

### 패키지가 임포트 안되는 경우
#### 동일한 패키지명이 존재할 경우 에러날 수도 있음(확인해보기)
* from google.cloud import storage
    ~~~shell
    $ pip install --upgrade google-cloud-storage
    ~~~
* from Crypto.Cipher import AES
    ~~~shell
    $ pip uninstall crypto
    $ pip uninstall pycrypto
    $ pip install pycrypto
    ~~~
* ModuleNotFoundError: No module named '_lzma'
    ~~~shell
    $ pyenv uninstall x.y.z
    $ brew install xz
    $ pyenv install x.y.z
    ~~~
  
