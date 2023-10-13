import os
import subprocess
import argparse
import concurrent.futures

def run_blast(input_file, db_folder,  output_file):
    # Comando blastn
    cmd = f"blastn -db {db_folder} -query {input_file} -evalue 1e-10 -max_target_seqs 2 -out {output_file} -outfmt '6 qseqid stitle pident qcovs length mismatch gapopen evalue bitscore'"
    subprocess.run(cmd, shell=True)

def move_file(file_path, destination):
    # Move o arquivo para a pasta de destino
    os.rename(file_path, os.path.join(destination, os.path.basename(file_path)))

def process_file(file, db_folder, folder, ready_folder):
    input_file = os.path.join(folder, file)
    output_file = os.path.join(ready_folder, f"{file}.txt")

    run_blast(input_file, db_folder, output_file)
    move_file(input_file, ready_folder)

def main(folder, db_folder):
    # Cria a pasta "pronto" dentro da pasta fornecida
    ready_folder = os.path.join(folder, "pronto")
    os.makedirs(ready_folder, exist_ok=True)

    # Obtém a lista de arquivos iniciados com "s" na pasta
    files = [f for f in os.listdir(folder) if f.startswith("s")]

    # Define o número máximo de threads ou processos (neste exemplo, usamos 10)
    max_workers = 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Executa o blastn para cada arquivo e move para a pasta "pronto"
        futures = []
        for file in files:
            futures.append(executor.submit(process_file, file, db_folder, folder, ready_folder))

        # Aguarda a conclusão de todas as tarefas
        concurrent.futures.wait(futures)

    print("Análises do BLAST concluídas e arquivos movidos para a pasta 'pronto'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executa análise BLAST em arquivos de uma pasta e move-os para a pasta 'pronto'")
    parser.add_argument("-in", "--input_folder", help="Caminho para a pasta de entrada")
    parser.add_argument("-db", "--db_folder", help="Caminho para o banco de dados")

    args = parser.parse_args()

    if args.input_folder:
        main(args.input_folder, args.db_folder)
    else:
        print("É necessário fornecer o caminho para a pasta de entrada. Use o argumento -in ou --input_folder.")
