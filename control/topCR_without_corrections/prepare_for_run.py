import os 
import shutil
def zip_files(list_of_files):
    if not os.path.exists("temp_folder"):
        os.makedirs("temp_folder")
    for file in list_of_files :
        shutil.copy(file,"temp_folder")
    archive_name = "helper_files"
    shutil.make_archive(archive_name,"zip","temp_folder")
    shutil.rmtree("temp_folder")
    return archive_name+".zip"
zip_files(
    [
        'snippets.py',
        'processor_Top.py'
    ]
)
