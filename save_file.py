import requests
import os


def mk_folder(folder_path:str):
    '''
    폴더 만들기
    '''
    try:
        os.makedirs(folder_path)  # 경로에 없는 상위 폴더까지 모두 생성
    except FileExistsError:
        pass
    except Exception as e:
        print(f"폴더 생성 중 오류가 발생했습니다: {e}")

def download_img(image_url:str, save_path:str):
    '''
    이미지 저장하기
    '''
    try:
        response = requests.get(image_url, stream=True)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
    except Exception as e:
        print(f"오류 발생: {e}")