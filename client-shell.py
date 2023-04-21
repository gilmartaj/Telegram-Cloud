from os.path import expanduser

from utilities import Cloud

auth = open(expanduser("~")+"/telegram-auth.txt", "r").read()
file_token = open(expanduser("~")+"/telegram-token.txt", "r").read().strip().split(",")
token = file_token[0]
hashc = file_token[1]
file_info = ".files_info.csv"

command_list = ["cd", "cdb", "csv", "dow", "exit", "help", "ls", "mkdir", "pwd", "rename", "up"]

def callback(current, total):
    print('Subidos', current//1024, 'kB de', total//1024,
          'kB: {:.2%}'.format(current / total))
          
def callback2(current, total):
    print('Baixados', current//1024, 'kB de', total//1024,
          'kB: {:.2%}'.format(current / total))

def read_command():
    print("tc-shell#:", end=" ", flush=True)
    c = input().strip()
    c1 = c.split()[0]
    c2 = "" if len(c1) == len(c) else c[len(c1):].strip()
    return (c1, c2)

current_directory = ""
cloud = Cloud(file_info, auth, token, hashc)

command, args = read_command()

while command != "exit":
    if not command in command_list:
        print("Comando inválido.")
    elif command == "cd":
        if cloud.exists_directory(current_directory + "/" + args):
            current_directory = current_directory + "/" + args
            print("Diretório alterado.")
        else:
            print("Diretório inexistente.")
    elif command == "cdb":
        if current_directory == "":
            print("Já está no diretório raiz.")
        else:
            current_directory = "/".join(current_directory.split("/")[:-1])
            print("Diretório alterado.")
    elif command == "csv":
        print(cloud.files_info_df)
    elif command == "dow":
        cloud.download_file(int(cloud.files_info_df._get_value(int(args), "file_id")), callback2, cloud.files_info_df._get_value(int(args), "filename_we")+cloud.files_info_df._get_value(int(args), "extension"))
    elif command == "help":
        print(command_list)
    elif command == "ls":
        print(cloud.get_directory_elements(current_directory))
    elif command == "mkdir":
        cloud.make_directory(current_directory + "/" + args)
    elif command == "pwd":
        print(f"'{current_directory}'")
    elif command == "rename":
        index = int(args.split()[0])
        name = "" if len(str(index)) == len(args) else args[len(str(index)):].strip()
        cloud.rename_element(index, name)
    elif command == "up":
        cloud.upload_file(args, current_directory, callback,True)
    cloud.write_files_info()
    command, args = read_command()
        
print("Programa encerrado")