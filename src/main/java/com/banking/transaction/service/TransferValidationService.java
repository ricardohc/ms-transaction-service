package com.banking.transaction.service;

import com.banking.transaction.client.SagaOrchestratorClient.AccountDto;
import com.banking.transaction.dto.AccountTransferTransaction;
import com.banking.transaction.exception.BusinessValidationException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class TransferValidationService {

    private static final Set<String> ALLOWED_TYPES = Set.of("SAVINGS", "CHECKED");
    private static final String REQUIRED_CURRENCY = "PEN";
    private static final int MONEY_SCALE = 2;
    private static final RoundingMode ROUNDING = RoundingMode.HALF_UP;

    private static final BigDecimal COMMISSION_RATE = new BigDecimal("0.005");

    public void validateRequest(final AccountTransferTransaction req) {
        if (req == null) {
            throw new BusinessValidationException("El payload es obligatorio");
        }
        if (req.getType() == null) {
            throw new BusinessValidationException("El campo 'type' es obligatorio");
        }
        if (req.getAmount() == null || req.getAmount().isBlank()) {
            throw new BusinessValidationException("El campo 'amount' es obligatorio");
        }
        if (req.getCurrency() == null || req.getCurrency().isBlank()) {
            throw new BusinessValidationException("El campo 'currency' es obligatorio");
        }
        if (req.getSourceAccountId() == null) {
            throw new BusinessValidationException(
                    "El campo 'sourceAccountId' es obligatorio");
        }
        if (req.getTargetAccountId() == null) {
            throw new BusinessValidationException(
                    "El campo 'targetAccountId' es obligatorio");
        }
        if (!REQUIRED_CURRENCY.equalsIgnoreCase(req.getCurrency())) {
            throw new BusinessValidationException(
                    "La moneda debe ser " + REQUIRED_CURRENCY);
        }

        final BigDecimal amount;
        try {
            amount = new BigDecimal(req.getAmount());
        } catch (NumberFormatException e) {
            throw new BusinessValidationException("El 'amount' no es un número válido");
        }
        if (amount.signum() <= 0) {
            throw new BusinessValidationException("El 'amount' debe ser mayor a cero");
        }
        // Normaliza escala a 2 decimales (evita "10.001" inyectado)
        if (amount.stripTrailingZeros().scale() > MONEY_SCALE) {
            throw new BusinessValidationException(
                    "El 'amount' no puede tener más de 2 decimales");
        }
    }

    public void validateAccounts(final AccountTransferTransaction req,
                                 final AccountDto source,
                                 final AccountDto target) {
        if (source == null || target == null) {
            throw new BusinessValidationException(
                    "No se pudieron cargar las cuentas de la transferencia");
        }
        if (req.getSourceAccountId().equals(req.getTargetAccountId())) {
            throw new BusinessValidationException(
                    "No se puede hacer transferencia en la misma cuenta");
        }
        if (!ALLOWED_TYPES.contains(source.getAccountType())) {
            throw new BusinessValidationException(
                    "No se puede hacer transferencia con cuentas de plazo fijo");
        }
        if (!ALLOWED_TYPES.contains(target.getAccountType())) {
            throw new BusinessValidationException(
                    "No se puede hacer transferencia con cuentas de plazo fijo");
        }
        if (source.getBalance() == null) {
            throw new BusinessValidationException(
                    "No se pudo obtener el saldo de la cuenta origen");
        }

        final BigDecimal amount = new BigDecimal(req.getAmount())
                .setScale(MONEY_SCALE, ROUNDING);
        final BigDecimal sourceBalance = source.getBalance()
                .setScale(MONEY_SCALE, ROUNDING);

        if (sourceBalance.compareTo(amount) < 0) {
            throw new BusinessValidationException(
                    "No tiene saldo suficiente para enviar el monto a enviar");
        }
    }

    public BigDecimal calculateCommission(final BigDecimal amount) {
        if (amount == null || amount.signum() <= 0) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, ROUNDING);
        }
        return amount.multiply(COMMISSION_RATE)
                .setScale(MONEY_SCALE, ROUNDING);
    }

    public BigDecimal calculateTotalDebit(final BigDecimal amount) {
        return amount.add(calculateCommission(amount))
                .setScale(MONEY_SCALE, ROUNDING);
    }

    public BigDecimal divideMoney(final BigDecimal total, final int parts) {
        if (parts <= 0) {
            throw new IllegalArgumentException("parts debe ser > 0");
        }
        if (total == null) {
            return BigDecimal.ZERO.setScale(MONEY_SCALE, ROUNDING);
        }
        return total.divide(BigDecimal.valueOf(parts), MONEY_SCALE, ROUNDING);
    }

    public BigDecimal convertCurrency(final BigDecimal amount,
                                      final BigDecimal exchangeRate) {
        if (amount == null || exchangeRate == null) {
            throw new IllegalArgumentException("amount y exchangeRate son obligatorios");
        }
        return amount.multiply(exchangeRate)
                .setScale(MONEY_SCALE, ROUNDING);
    }

    public BigDecimal calculatePercentage(final BigDecimal amount,
                                          final BigDecimal percent) {
        if (amount == null || percent == null) {
            throw new IllegalArgumentException("amount y percent son obligatorios");
        }
        // percent = 0.005 significa 0.5%
        return amount.multiply(percent)
                .setScale(MONEY_SCALE, ROUNDING);
    }
}