from modules import generate_ozon_file, generate_wb_file, update_wb_prices

if __name__ == "__main__":
    print("Тестовый запуск модулей PriceCraft")
    generate_ozon_file.main()
    generate_wb_file.main()
    update_wb_prices.main()
