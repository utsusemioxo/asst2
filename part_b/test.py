import subprocess

def run_commands():
    try:
        print("./runtasks -n 8 mandelbrot_chunked")
        subprocess.run(["./runtasks", "-n", "8", "mandelbrot_chunked"], check=True)
        
        # todo: more tests here    
    except subprocess.CalledProcessError as e:
        print("Error occurred while executing a command: {e}")

if __name__ == "__main__":
    run_commands()
    