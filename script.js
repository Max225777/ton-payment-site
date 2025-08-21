 Ініціалізація TonConnect
const tonConnectUI = new TON_CONNECT_UI.TonConnectUI({
    manifestUrl: 'https://max225777.github.io/ton-payment-site/tonconnect-manifest.json'
});
// Елементи DOM
const connectBtn = document.getElementById('connect-btn');
const buyBtn = document.getElementById('buy-btn');
const walletInfo = document.getElementById('wallet-info');
const status = document.getElementById('status');
// Перевірка чи гаманець вже підключений
async function checkConnection() {
    const connected = tonConnectUI.connected;
    
    if (connected) {
        await walletConnected();
    }
}
// Підключення гаманця
connectBtn.addEventListener('click', async () => {
    try {
        connectBtn.disabled = true;
        connectBtn.textContent = "Підключення...";
        
        await tonConnectUI.connectWallet();
        await walletConnected();
        
    } catch (error) {
        console.error("Помилка підключення:", error);
        showStatus("Помилка підключення гаманця", "error");
        connectBtn.disabled = false;
        connectBtn.textContent = "Підключити гаманець";
    }
});
// Обробка підключеного гаманця
async function walletConnected() {
    const wallet = tonConnectUI.wallet;
    
    if (wallet) {
        connectBtn.style.display = "none";
        buyBtn.style.display = "flex";
        
        // Показуємо інформацію про гаманець
        const shortAddress = `${wallet.account.address.slice(0, 6)}...${wallet.account.address.slice(-6)}`;
        walletInfo.textContent = `Підключено: ${shortAddress}`;
        walletInfo.style.display = "block";
        
        showStatus("Гаманець успішно підключено", "success");
    }
}
// Обробка кнопки купівлі
buyBtn.addEventListener('click', async () => {
    try {
        buyBtn.disabled = true;
        buyBtn.textContent = "Обробка транзакції...";
        
        // Створюємо транзакцію
        const transaction = {
            validUntil: Date.now() + 1000000,
            messages: [
                {
                    address: "UQDYSduMmfE6sBBBhvTR1wGtTG0_hmsj_xRaIg7P_W6-Pv1y", // Замініть на вашу адресу
                    amount: "1000000000" // 1 TON в наноТОН
                }
            ]
        };
        
        // Відправляємо транзакцію
        const result = await tonConnectUI.sendTransaction(transaction);
        
        showStatus("Транзакція успішно виконана! Дякуємо за покупку.", "success");
        console.log("Transaction result:", result);
        
    } catch (error) {
        console.error("Помилка транзакції:", error);
        
        if (error?.message?.includes("User rejection")) {
            showStatus("Транзакцію скасовано", "error");
        } else {
            showStatus("Помилка при виконанні транзакції", "error");
        }
    } finally {
        buyBtn.disabled = false;
        buyBtn.textContent = "Купити за 1 TON";
    }
});
// Функція для відображення статусу
function showStatus(message, type) {
    status.textContent = message;
    status.className = "status";
    status.classList.add(type);
    status.style.display = "block";
    
    // Автоматично приховати повідомлення через 5 секунд
    setTimeout(() => {
        status.style.display = "none";
    }, 5000);
}
// Перевіряємо стан підключення при завантаженні сторінки
document.addEventListener('DOMContentLoaded', checkConnection);
