"""
Apartado 3
Se dan distintos ficheros y calcula los triciclos de cada uno de los ficheros en paralelo.
Primero ejecutarlo y despues indicar los grafos en el que se quieren calcular los triciclos
Misma idea que en el apartado 2
"""
import sys
from pyspark import SparkContext
sc = SparkContext()


#Ordena las aristas
def mapper(line):
    n1,n2 = line[0],line[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2

def get_rdd_distict_edges(sc, info):
    return info.\
        map(mapper).\
        filter(lambda x: x is not None).\
        distinct()

def adjacents(sc, info):
    nodes = get_rdd_distict_edges(sc, info)
    adj = nodes.groupByKey()
    return adj



def funcion_aux(tupla):
    resultado = []
    nodo,lista_adj = tupla[0],list(tupla[1])
    for elem in lista_adj:
        if nodo <= elem:
            tup = nodo,elem
        else:
            tup = elem,nodo
        tupl = tup,"exists"
        resultado.append(tupl)
    for n in range(len(lista_adj)):
        for m in range((n + 1), len(lista_adj)):
            if lista_adj[n] <= lista_adj[m]:
                nuevo = lista_adj[n],lista_adj[m]
            else:
                nuevo = lista_adj[m],lista_adj[n]
            nuevo2 = "pending",nodo
            nuevo3 = nuevo,nuevo2
            resultado.append(nuevo3)
    return resultado

def filtro(tupla):
    resultado1 = False
    resultado2 = False
    for i in list(tupla[1]):
        if i == "exists":
            resultado1 = True
        else:
            resultado2 = True
    return (resultado1 and resultado2)

#Obtiene el triciclo
def obtener_ciclo(tupla):
    resultado = []
    triciclo = [tupla[0][0],tupla[0][1]]
    for i in list(tupla[1]):
        if i != "exists":
            triciclo.append(i[1])
            resultado.append(triciclo)
            triciclo = [tupla[0][0],tupla[0][1]]
    return resultado
        
#Aplicamos orden superior
def triciclos(sc,files):
    rdd = sc.parallelize([])
    for file_name in files:
        file_rdd = sc.textFile(file_name).\
            map(lambda x: ((x[0], file_name),(x[2],file_name)))
        rdd = rdd.union(file_rdd)
    resultado = adjacents(sc,rdd).\
        flatMap(funcion_aux).\
        groupByKey().\
        filter(filtro).\
        flatMap(obtener_ciclo).collect()
    final_resultado = {}
    for elem in resultado:
        archivo = elem[0][1]
        if archivo in final_resultado:
            ciclo = []
            for nodes in elem:
                ciclo.append(nodes[0])
            final_resultado[archivo].append(ciclo)
        else:
            ciclo = []
            for nodes in elem:
                ciclo.append(nodes[0])
            final_resultado[archivo]=[ciclo]
    return final_resultado



def main(sc,file):
   print(triciclos(sc,files))
    
if __name__ =="__main__":
    if len(sys.argv) < 2:
        print('Dame el primer grafo:')
        grafo1=input()
        print('Dame el segundo grafo:')
        grafo2=input()
        files=[grafo1,grafo2]
    else:
        files = list(sys.argv[1][1:-1].split(","))
    main(sc,files)

#print(triciclos(sc,["g2.txt","g6.txt"]))
#print(triciclos(sc,["g3.txt"]))