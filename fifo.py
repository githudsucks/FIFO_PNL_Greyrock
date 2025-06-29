import pandas as pd
from collections import defaultdict
import os
import sys

def calculate_fifo_pnl(csv_file):
    """
    Calculate FIFO PnL from a CSV file containing bond trades with support for short positions.
    
    Args:
        csv_file: Path to CSV file with columns ['Contract', 'Price', 'Quantity']
        
    Returns:
        Tuple of (realized_pnl, remaining_positions_df, trade_history_df)
    """
    # Debug: Verify file exists
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found at: {csv_file}")
    
    # Read CSV with error handling
    try:
        df = pd.read_csv(csv_file)
        df['Contract'] = df['Contract'].str.strip()
        df['Quantity'] = df['Quantity'].astype(float)  # Ensure numeric
        df['Price'] = df['Price'].astype(float)
    except Exception as e:
        raise ValueError(f"Error reading CSV: {str(e)}")
    
    # Validate required columns
    required_cols = {'Contract', 'Price', 'Quantity'}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"CSV missing required columns: {missing}")

    positions = defaultdict(list)  # {contract: [(price, quantity), ...]} (long positions)
    short_positions = defaultdict(list)  # {contract: [(price, quantity), ...]} (short positions)
    realized_pnl = 0.0
    trade_history = []

    for _, row in df.iterrows():
        contract = row['Contract']
        price = row['Price']
        quantity = row['Quantity']
        
        if pd.isna(price) or pd.isna(quantity):
            continue  # Skip rows with missing data

        if quantity > 0:  # Buy
            remaining_buy_quantity = quantity
            
            # First cover any short positions (FIFO)
            while remaining_buy_quantity > 0 and short_positions.get(contract):
                first_short_price, first_short_quantity = short_positions[contract][0]
                
                if first_short_quantity <= remaining_buy_quantity:
                    # Entire short position is covered
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
                    # Partial short position is covered
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
            
            # Add remaining to long positions
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
                
        else:  # Sell (negative quantity)
            remaining_sell_quantity = -quantity
            
            # First sell from long positions (FIFO)
            while remaining_sell_quantity > 0 and positions.get(contract):
                first_buy_price, first_buy_quantity = positions[contract][0]
                
                if first_buy_quantity <= remaining_sell_quantity:
                    # Entire position is closed
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
                    # Partial position is closed
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
            
            # If still remaining, create short position
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

    # Prepare remaining positions report (both long and short)
    remaining_data = []
    for contract, lots in positions.items():
        for price, quantity in lots:
            if quantity > 0:
                remaining_data.append({
                    'Position': 'LONG',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': quantity,
                    'Cost Basis': price * quantity
                })
    
    for contract, lots in short_positions.items():
        for price, quantity in lots:
            if quantity > 0:
                remaining_data.append({
                    'Position': 'SHORT',
                    'Contract': contract,
                    'Price': price,
                    'Quantity': quantity,
                    'Cost Basis': price * quantity
                })
    
    remaining_positions_df = pd.DataFrame(remaining_data) if remaining_data else pd.DataFrame()
    
    # Prepare trade history report
    trade_history_df = pd.DataFrame(trade_history)
    
    return realized_pnl, remaining_positions_df, trade_history_df

def main():
    print("=== FIFO PnL Calculator with Short Position Support ===")
    
    # Debug: Show current directory
    print(f"\nCurrent working directory: {os.getcwd()}")
    print("Files in directory:", [f for f in os.listdir() if f.endswith('.csv')])
    
    # Get input file path
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    else:
        input_file = os.path.join(os.getcwd(), "bond_trades.csv")
    
    print(f"\nLooking for input file at: {input_file}")
    
    try:
        pnl, remaining_positions, trade_history = calculate_fifo_pnl(input_file)
        
        print("\n=== RESULTS ===")
        print(f"\nTotal Realized PnL: ${pnl:,.2f}")
        
        print("\n=== TRADE HISTORY ===")
        print(trade_history.to_string(index=False, float_format="%.2f"))
        
        print("\n=== REMAINING POSITIONS ===")
        if not remaining_positions.empty:
            print("\nDetailed Positions:")
            print(remaining_positions.to_string(index=False, float_format="%.2f"))
            
            # Calculate aggregate position info
            position_summary = remaining_positions.groupby(['Contract', 'Position']).agg(
                Total_Quantity=('Quantity', 'sum'),
                Avg_Price=('Price', 'mean'),
                Total_Cost=('Cost Basis', 'sum')
            ).reset_index()
            
            print("\nPosition Summary:")
            print(position_summary.to_string(index=False, float_format="%.2f"))
        else:
            print("No remaining positions")
        
        # Generate output files
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "pnl_results"
        os.makedirs(output_dir, exist_ok=True)
        
        trade_history.to_csv(
            os.path.join(output_dir, f"trade_history_{timestamp}.csv"), 
            index=False,
            float_format="%.2f"
        )
        remaining_positions.to_csv(
            os.path.join(output_dir, f"remaining_positions_{timestamp}.csv"), 
            index=False,
            float_format="%.2f"
        )
        
        print(f"\nReports saved to: {os.path.abspath(output_dir)}")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Verify the CSV file exists at the specified path")
        print("2. Check the CSV has columns: Contract, Price, Quantity")
        print("3. Ensure quantity values are numeric (negative for sales)")
        print("4. Try using the full absolute path to your CSV file")

if __name__ == "__main__":
    main()