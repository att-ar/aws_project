from inspect import getmembers, isfunction, isclass

def display_callables(module):
    '''Display the callable names (classes, functions) in a module'''
    classes = getmembers(module, isclass)
    print("----------")
    print(f"{module.__name__} classes:")
    for c in classes:
        if c[1].__module__ == module.__name__ and c[0][:6] != "helper":
            print(f"\t{c[0]} methods:", end="")
            methods = [m[0] for m in getmembers(c[1], isfunction)]
            print("", *methods, sep="\n\t\t")
    functions = [f[0] for f in getmembers(module, isfunction)
                 if f[1].__module__ == module.__name__ and f[0][:6] != "helper"]
    print(f"{module.__name__} functions:", *functions, sep="\n\t")
    print("----------")
    return None
