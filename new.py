import pandas as pd
from collections import defaultdict
import os
import sys
from datetime import datetime

def calculate_fifo_pnl(csv_file, eom_prices=None):
    """
    Calculate FIFO PnL with end-of-month settlement pricing support.
    
    Args:
        csv_file: Path to CSV file with trades
        eom_prices: Dict of {contract: price} for EOM settlement
        
    Returns:
        Tuple of (realized_pnl, unrealized_pnl, remaining_positions_df, trade_history_df)
    """
    # File reading and validation (same as before)
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found at: {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
        df['Contract'] = df['Contract'].str.strip()
        df['Quantity'] = df['Quantity'].astype(float)
        df['Price'] = df['Price'].astype(float)
    except Exception as e:
        raise ValueError(f"Error reading CSV: {str(e)}")

    required_cols = {'Contract', 'Price', 'Quantity'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"CSV missing required columns: {missing}")

    # Initialize tracking
    positions = defaultdict(list)
    short_positions = defaultdict(list)
    realized_pnl = 0.0
    trade_history = []

    # Process trades (same FIFO logic as before)
    for _, row in df.iterrows():
        contract = row['Contract']
        price = row['Price']
        quantity = row['Quantity']
        
        if pd.isna(price) or pd.isna(quantity):
            continue

        if quantity > 0:  # Buy
            remaining_buy_quantity = quantity
            
            # Cover short positions first
            while remaining_buy_quantity > 0 and short_positions.get(contract):
                first_short_price, first_short_quantity = short_positions[contract][0]
                
                if first_short_quantity <= remaining_buy_quantity:
                    lot_pnl = (first_short_price - price) * first_short_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'COVER',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': first_short_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Covered {first_short_quantity} @ {first_short_price}"
                    })
                    remaining_buy_quantity -= first_short_quantity
                    short_positions[contract].pop(0)
                else:
                    lot_pnl = (first_short_price - price) * remaining_buy_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'COVER',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': remaining_buy_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Covered {remaining_buy_quantity} @ {first_short_price}"
                    })
                    short_positions[contract][0] = (
                        first_short_price, 
                        first_short_quantity - remaining_buy_quantity
                    )
                    remaining_buy_quantity = 0
            
            if remaining_buy_quantity > 0:
                positions[contract].append((price, remaining_buy_quantity))
                trade_history.append({
                    'Type': 'BUY',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': remaining_buy_quantity,
                    'PnL': 0,
                    'Remaining': f"{contract} {remaining_buy_quantity} @ {price}"
                })
                
        else:  # Sell
            remaining_sell_quantity = -quantity
            
            while remaining_sell_quantity > 0 and positions.get(contract):
                first_buy_price, first_buy_quantity = positions[contract][0]
                
                if first_buy_quantity <= remaining_sell_quantity:
                    lot_pnl = (price - first_buy_price) * first_buy_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'SELL',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': first_buy_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Matched {first_buy_quantity} @ {first_buy_price}"
                    })
                    remaining_sell_quantity -= first_buy_quantity
                    positions[contract].pop(0)
                else:
                    lot_pnl = (price - first_buy_price) * remaining_sell_quantity
                    realized_pnl += lot_pnl
                    trade_history.append({
                        'Type': 'SELL',
                        'Contract': contract,
                        'Price': price,
                        'Quantity': remaining_sell_quantity,
                        'PnL': lot_pnl,
                        'Remaining': f"Matched {remaining_sell_quantity} @ {first_buy_price}"
                    })
                    positions[contract][0] = (
                        first_buy_price, 
                        first_buy_quantity - remaining_sell_quantity
                    )
                    remaining_sell_quantity = 0
            
            if remaining_sell_quantity > 0:
                short_positions[contract].append((price, remaining_sell_quantity))
                trade_history.append({
                    'Type': 'SHORT',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': remaining_sell_quantity,
                    'PnL': 0,
                    'Remaining': f"Short {remaining_sell_quantity} @ {price}"
                })

    # Calculate unrealized PnL using EOM prices
    unrealized_pnl = 0.0
    remaining_data = []
    
    for contract, lots in positions.items():
        eom_price = eom_prices.get(contract) if eom_prices else None
        
        for price, quantity in lots:
            if quantity > 0:
                cost_basis = price * quantity
                market_value = eom_price * quantity if eom_price else None
                unrealized = (eom_price - price) * quantity if eom_price else 0
                
                remaining_data.append({
                    'Position': 'LONG',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': quantity,
                    'Cost Basis': cost_basis,
                    'EOM Price': eom_price,
                    'Market Value': market_value,
                    'Unrealized PnL': unrealized
                })
                unrealized_pnl += unrealized
    
    for contract, lots in short_positions.items():
        eom_price = eom_prices.get(contract) if eom_prices else None
        
        for price, quantity in lots:
            if quantity > 0:
                cost_basis = price * quantity
                market_value = eom_price * quantity if eom_price else None
                unrealized = (price - eom_price) * quantity if eom_price else 0
                
                remaining_data.append({
                    'Position': 'SHORT',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': quantity,
                    'Cost Basis': cost_basis,
                    'EOM Price': eom_price,
                    'Market Value': market_value,
                    'Unrealized PnL': unrealized
                })
                unrealized_pnl += unrealized
    
    remaining_positions_df = pd.DataFrame(remaining_data) if remaining_data else pd.DataFrame()
    trade_history_df = pd.DataFrame(trade_history)
    
    return realized_pnl, unrealized_pnl, remaining_positions_df, trade_history_df

def main():
    print("=== Enhanced FIFO PnL Calculator with EOM Pricing ===")
    
    # Get input file path
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = os.path.join(os.getcwd(), "bond_trades.csv")
    
    # Load EOM prices (could also load from a separate CSV)
    eom_prices = {
        'May 3%': 60.25,
        'June 3%': 58.80,
        # Add other contracts as needed
    }
    
    try:
        realized, unrealized, remaining, history = calculate_fifo_pnl(input_file, eom_prices)
        
        print("\n=== RESULTS ===")
        print(f"Realized PnL:   ${realized:,.2f}")
        print(f"Unrealized PnL: ${unrealized:,.2f}")
        print(f"Total PnL:      ${realized + unrealized:,.2f}")
        
        print("\n=== TRADE HISTORY ===")
        print(history.to_string(index=False, float_format="%.2f"))
        
        print("\n=== REMAINING POSITIONS ===")
        if not remaining.empty:
            print("\nDetailed Positions:")
            print(remaining.to_string(index=False, float_format="%.2f"))
            
            # Summary by contract
            summary = remaining.groupby(['Contract', 'Position']).agg(
                Quantity=('Quantity', 'sum'),
                Avg_Price=('Price', 'mean'),
                Total_Cost=('Cost Basis', 'sum'),
                Total_Unrealized=('Unrealized PnL', 'sum')
            ).reset_index()
            
            print("\nPosition Summary:")
            print(summary.to_string(index=False, float_format="%.2f"))
        else:
            print("No remaining positions")
        
        # Generate comprehensive report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "pnl_reports"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save reports with additional metadata
        with open(os.path.join(output_dir, f"pnl_report_{timestamp}.txt"), 'w') as f:
            f.write(f"FIFO PnL Report - {timestamp}\n")
            f.write(f"Input File: {input_file}\n")
            f.write("\n=== SUMMARY ===\n")
            f.write(f"Realized PnL:   ${realized:,.2f}\n")
            f.write(f"Unrealized PnL: ${unrealized:,.2f}\n")
            f.write(f"Total PnL:      ${realized + unrealized:,.2f}\n")
            
            if not remaining.empty:
                f.write("\n=== OPEN POSITIONS ===\n")
                f.write(remaining.to_string(index=False, float_format="%.2f"))
        
        # Save CSV versions
        remaining.to_csv(
            os.path.join(output_dir, f"positions_{timestamp}.csv"),
            index=False,
            float_format="%.2f"
        )
        history.to_csv(
            os.path.join(output_dir, f"trades_{timestamp}.csv"),
            index=False,
            float_format="%.2f"
        )
        
        print(f"\nFull report saved to: {os.path.abspath(output_dir)}")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Verify input file exists and format is correct")
        print("2. Check all required columns are present")
        print("3. Ensure EOM prices are provided for all contracts")

if __name__ == "__main__":
    main()