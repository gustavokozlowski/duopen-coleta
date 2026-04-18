#!/usr/bin/env python3
"""
Script helper para executar testes da coleta de Macaé.
Uso: python run_tests.py [opção]
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent

def run_command(cmd, description=""):
    """Executa comando e mostra resultado."""
    if description:
        print(f"\n{'='*70}")
        print(f"➜ {description}")
        print('='*70)
    
    result = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT)
    return result.returncode

def main():
    """Menu de execução de testes."""
    if len(sys.argv) > 1:
        option = sys.argv[1]
    else:
        print("\n🧪 Teste - Coleta de Macaé\n")
        print("Opções:")
        print("  1 - Executar TODOS os testes")
        print("  2 - Testes unitários apenas")
        print("  3 - Testes de integração apenas")
        print("  4 - Testes com cobertura de código")
        print("  5 - Teste rápido (smoke test)")
        print("  6 - Testes do painel legado")
        print("  q - Sair")
        print()
        option = input("Escolha uma opção: ").strip()
    
    if option == "1":
        run_command(
            "python -m pytest tests/ -v",
            "Executando TODOS os testes"
        )
    
    elif option == "2":
        run_command(
            "python -m pytest tests/test_portal_macae.py tests/unit/macae/test_painel_legado_unit.py -v",
            "Executando testes unitários da coleta de Macaé"
        )
    
    elif option == "3":
        run_command(
            "python -m pytest tests/integration/macae/test_portal_macae_selenium.py -v",
            "Executando 1 teste de integração"
        )
    
    elif option == "4":
        # Instalar pytest-cov se não estiver
        subprocess.run("pip install -q pytest-cov", shell=True, cwd=PROJECT_ROOT)
        run_command(
            "python -m pytest tests/ --cov=etl --cov=scrappers --cov-report=term-missing --cov-report=html",
            "Executando testes com cobertura de código"
        )
        print("\n✅ Relatório HTML gerado em 'htmlcov/index.html'")
    
    elif option == "5":
        run_command(
            "python -m pytest tests/test_portal_macae.py::TestConfiguração -v",
            "Smoke test - Verificando configuração"
        )

    elif option == "6":
        run_command(
            "python -m pytest tests/unit/macae/test_painel_legado_unit.py -v",
            "Executando testes do painel legado"
        )
    
    elif option.lower() in ['q', 'quit', 'exit']:
        print("Saindo...")
        return 0
    
    else:
        print(f"❌ Opção '{option}' inválida")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
