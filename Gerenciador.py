from Database import Database
from typing import List, Dict, Any, Optional
import uuid

class GerenciadorPartidas:
    def __init__(self, db_instance):
        self.db = db_instance
        self.criar_constraints()
    
    def criar_constraints(self):
        self.db.execute_query("""
            CREATE CONSTRAINT jogador_id_unique IF NOT EXISTS
            FOR (p:Jogador) REQUIRE p.id IS UNIQUE
        """)
        
        self.db.execute_query("""
            CREATE CONSTRAINT partida_id_unique IF NOT EXISTS
            FOR (p:Partida) REQUIRE p.id IS UNIQUE
        """)

    def criar_jogador(self, nome) -> str:
        jogador_id = str(uuid.uuid4())
        query = "CREATE (j:Jogador {id: $id, nome: $nome})"
        parameters = {"id": jogador_id, "nome": nome}
        self.db.execute_query(query, parameters)
        return jogador_id
    
    def buscar_jogador_por_id(self, jogador_id) -> Optional[Dict[str, Any]]:
        query = "MATCH (j:Jogador {id: $id}) RETURN j"
        parameters = {"id": jogador_id}
        results = self.db.execute_query(query, parameters)
        
        if results:
            return dict(results[0]["j"]) 
        return None
    
    def buscar_todos_jogadores(self) -> List[Dict[str, Any]]:
        query = "MATCH (j:Jogador) RETURN j"
        results = self.db.execute_query(query)
        return [dict(record["j"]) for record in results]
    
    def atualizar_jogador(self, jogador_id, nome) -> bool:
        query = "MATCH (j:Jogador {id: $id}) SET j.nome = $nome RETURN j"
        parameters = {"id": jogador_id, "nome": nome}
        results = self.db.execute_query(query, parameters)
        return len(results) > 0 
    
    def excluir_jogador(self, jogador_id) -> bool:
        query = """
            MATCH (j:Jogador {id: $id})
            OPTIONAL MATCH (j)-[r]-()
            DELETE r, j
            """
        parameters = {"id": jogador_id}
        self.db.execute_query(query, parameters)
        return True 
    
    def criar_partida(self) -> str:
        partida_id = str(uuid.uuid4())
        query = "CREATE (p:Partida {id: $id, data: datetime()})"
        parameters = {"id": partida_id}
        self.db.execute_query(query, parameters)
        return partida_id
    
    def adicionar_jogador_partida(self, partida_id, jogador_id) -> bool:
        query = """
            MATCH (j:Jogador {id: $jogador_id})
            MATCH (p:Partida {id: $partida_id})
            MERGE (j)-[:PARTICIPOU]->(p)
            RETURN j, p 
            """
        parameters = {"jogador_id": jogador_id, "partida_id": partida_id}
        results = self.db.execute_query(query, parameters)
        return len(results) > 0
    
    def registrar_resultado_partida(self, partida_id, resultados: Dict[str, int]) -> bool:
        for jogador_id, pontuacao in resultados.items():
            query_score = """
                MATCH (j:Jogador {id: $jogador_id})
                MATCH (p:Partida {id: $partida_id})
                MERGE (j)-[r:PARTICIPOU]->(p)
                SET r.pontuacao = $pontuacao
                """
            parameters_score = {
                "jogador_id": jogador_id, 
                "partida_id": partida_id, 
                "pontuacao": pontuacao
            }
            self.db.execute_query(query_score, parameters_score)
        
        if resultados:
            vencedor_id = max(resultados.items(), key=lambda x: x[1])[0]
            query_winner = """
                MATCH (p:Partida {id: $partida_id})
                SET p.vencedor_id = $vencedor_id 
                """
            parameters_winner = {"partida_id": partida_id, "vencedor_id": vencedor_id}
            self.db.execute_query(query_winner, parameters_winner)
        
        return True 
    
    def obter_partida(self, partida_id) -> Optional[Dict[str, Any]]:
        query_partida = "MATCH (p:Partida {id: $id}) RETURN p"
        parameters_partida = {"id": partida_id}
        result_partida_list = self.db.execute_query(query_partida, parameters_partida)
        
        if not result_partida_list:
            return None
        
        info_partida = dict(result_partida_list[0]["p"])
        
        query_jogadores = """
            MATCH (j:Jogador)-[r:PARTICIPOU]->(p:Partida {id: $id})
            RETURN j.id as jogador_id, j.nome as nome_jogador, r.pontuacao as pontuacao
            """
        parameters_jogadores = {"id": partida_id}
        results_jogadores_list = self.db.execute_query(query_jogadores, parameters_jogadores)
        
        info_partida["jogadores"] = [
            {
                "jogador_id": record["jogador_id"],
                "nome_jogador": record["nome_jogador"],
                "pontuacao": record["pontuacao"]
            }
            for record in results_jogadores_list
        ]
        
        return info_partida
    
    def obter_todas_partidas(self) -> List[Dict[str, Any]]:
        query_all_partidas = "MATCH (p:Partida) RETURN p"
        results_partidas = self.db.execute_query(query_all_partidas)
        partidas_list = []
        
        for partida_record in results_partidas:
            info_partida = dict(partida_record["p"])
            partida_id = info_partida["id"]
            
            query_jogadores = """
                MATCH (j:Jogador)-[r:PARTICIPOU]->(p:Partida {id: $id})
                RETURN j.id as jogador_id, j.nome as nome_jogador, r.pontuacao as pontuacao
                """
            parameters_jogadores = {"id": partida_id}
            results_jogadores_list = self.db.execute_query(query_jogadores, parameters_jogadores)
            
            info_partida["jogadores"] = [
                {
                    "jogador_id": record["jogador_id"],
                    "nome_jogador": record["nome_jogador"],
                    "pontuacao": record["pontuacao"]
                }
                for record in results_jogadores_list
            ]
            partidas_list.append(info_partida)
        
        return partidas_list
    
    def excluir_partida(self, partida_id) -> bool:
        query = """
            MATCH (p:Partida {id: $id})
            OPTIONAL MATCH (p)-[r]-() 
            DELETE r, p
            """
        query_detach = "MATCH (p:Partida {id: $id}) DETACH DELETE p"
        parameters = {"id": partida_id}
        self.db.execute_query(query_detach, parameters)
        return True
    
    def buscar_historico_partidas_jogador(self, jogador_id) -> List[Dict[str, Any]]:
        query_historico = """
            MATCH (j:Jogador {id: $id})-[r:PARTICIPOU]->(p:Partida)
            RETURN p, r.pontuacao as pontuacao_jogador
            ORDER BY p.data DESC
            """
        parameters_historico = {"id": jogador_id}
        results_historico = self.db.execute_query(query_historico, parameters_historico)
        
        partidas_jogador = []
        for record in results_historico:
            info_partida = dict(record["p"])
            info_partida["pontuacao_jogador"] = record["pontuacao_jogador"]
            info_partida["e_vencedor"] = info_partida.get("vencedor_id") == jogador_id
            
            partida_id = info_partida["id"]
            query_jogadores_partida = """
                MATCH (player:Jogador)-[rel:PARTICIPOU]->(game:Partida {id: $id})
                RETURN player.id as jogador_id, player.nome as nome_jogador, rel.pontuacao as pontuacao
                """
            parameters_jogadores_partida = {"id": partida_id}
            jogadores_na_partida = self.db.execute_query(query_jogadores_partida, parameters_jogadores_partida)
            
            info_partida["jogadores"] = [
                {
                    "jogador_id": j_rec["jogador_id"],
                    "nome_jogador": j_rec["nome_jogador"],
                    "pontuacao": j_rec["pontuacao"]
                }
                for j_rec in jogadores_na_partida
            ]
            partidas_jogador.append(info_partida)
            
        return partidas_jogador

def main():
    uri = "bolt://localhost:7687" 
    usuario = "neo4j"
    senha = "senha123"  
    db = Database(uri, usuario, senha)
    gerenciador = GerenciadorPartidas(db)
    
    try:
        print("Criando jogadores...")
        jogador1_id = gerenciador.criar_jogador("Braian")
        jogador2_id = gerenciador.criar_jogador("Mérida")
        jogador3_id = gerenciador.criar_jogador("Clériton")
        jogador4_id = gerenciador.criar_jogador("Matue")
        
        print("\nJogadores cadastrados:")
        jogadores = gerenciador.buscar_todos_jogadores()
        for j_info in jogadores: 
            print(f"- {j_info['nome']} (ID: {j_info['id']})")
        
        print("\nCriando partida...")
        partida_id = gerenciador.criar_partida()
        print(f"Partida criada com ID: {partida_id}")
        
        print("Adicionando jogadores à partida...")
        gerenciador.adicionar_jogador_partida(partida_id, jogador1_id)
        gerenciador.adicionar_jogador_partida(partida_id, jogador2_id)
        gerenciador.adicionar_jogador_partida(partida_id, jogador3_id)
        gerenciador.adicionar_jogador_partida(partida_id, jogador4_id)
        
        print("Registrando resultados...")
        resultados_partida = { 
            jogador1_id: 56,
            jogador2_id: 85,
            jogador3_id: 12,
            jogador4_id: 654
        }
        gerenciador.registrar_resultado_partida(partida_id, resultados_partida)
        
        print("\nInformações da partida:")
        partida_info = gerenciador.obter_partida(partida_id) 
        if partida_info:
            print(f"ID: {partida_info['id']}")
            print(f"Data: {partida_info['data']}")
            print("Jogadores:")
            if "jogadores" in partida_info:
                for jogador_partida in partida_info["jogadores"]: 
                    print(f"- {jogador_partida['nome_jogador']}: {jogador_partida.get('pontuacao', 'N/A')} pontos")
            vencedor_id = partida_info.get("vencedor_id")
            if vencedor_id:
                vencedor = gerenciador.buscar_jogador_por_id(vencedor_id)
                if vencedor:
                    print(f"Vencedor: {vencedor['nome']}")
        else:
            print("Partida não encontrada.")
        
        print("\nHistórico de partidas do jogador Maria:")
        historico_maria = gerenciador.buscar_historico_partidas_jogador(jogador1_id) 
        for p_hist in historico_maria: 
            status = "Venceu" if p_hist.get("e_vencedor") else ("Empate/Não finalizada" if p_hist.get("vencedor_id") is None else "Perdeu")
            print(f"- Partida {p_hist['id']}: {p_hist.get('pontuacao_jogador', 'N/A')} pontos ({status})")

        print("\nTestando exclusão:")
        print(f"Excluindo jogador João (ID: {jogador2_id})...")
        gerenciador.excluir_jogador(jogador2_id)
        jogador_joao = gerenciador.buscar_jogador_por_id(jogador2_id)
        print(f"João encontrado após exclusão: {'Sim' if jogador_joao else 'Não'}")

        print(f"Excluindo partida (ID: {partida_id})...")
        gerenciador.excluir_partida(partida_id)
        partida_excluida_info = gerenciador.obter_partida(partida_id)
        print(f"Partida encontrada após excluir: {'Sim' if partida_excluida_info else 'Não'}")

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        db.close()
        print("\nConexão fechada.")

if __name__ == "__main__":
    main()